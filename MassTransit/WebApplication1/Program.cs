using MassTransit;
using MassTransit.DapperIntegration;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Data;
using Dapper;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;

var builder = WebApplication.CreateBuilder(args);

var connectionString = builder.Configuration.GetConnectionString("SqlServer");

builder.Services.AddSingleton<IDbConnection>(new SqlConnection(connectionString));

builder.Services.AddControllers();

builder.Services.AddMassTransit(cfg =>
{
    // Configure the bus with the Dapper persistence
    cfg.SetEndpointNameFormatter(KebabCaseEndpointNameFormatter.Instance);
    cfg.UsingSqlServer((context, cfg) =>
    {
        cfg.Host(context.Configuration.GetConnectionString("PaymentService"));
        cfg.UseDapper(dp =>
        {
            dp.Options.SchemaName = "dbo";
            dp.ConnectionFactory = DbConnectionFactoryProvider.Create(context.Configuration.GetConnectionString("PaymentService"));
        });
    });

    // Configure the sagas
    cfg.AddSagaStateMachine<OrderSagaStateMachine, OrderSagaState>()
        .DapperRepository(r =>
        {
            r.ConnectionProvider = (provider) => DbConnectionFactoryProvider.Create(provider.GetService<IConfiguration>().GetConnectionString("PaymentService"));
        });

    // Configure the consumers
    cfg.AddConsumer<PaymentSystemCallbackConsumer>();
    cfg.AddConsumer<CompletedCallbackConsumer>();

    // Configure the retry policy
    cfg.UseRetry(r => r.Interval(3, TimeSpan.FromSeconds(10)));

    // Configure the message topology
    cfg.Message<OrderSagaCommand>(x => x.SetEntityName("order-saga-commands"));
    cfg.Message<CompletedCallback>(x => x.SetEntityName("completed-callbacks"));
    cfg.Message<PaymentSystemCallback>(x => x.SetEntityName("payment-system-callbacks"));
});

builder.Services.AddMassTransitHostedService();

var app = builder.Build();

app.UseRouting();

app.MapControllers();

app.Run();

// PaymentController.cs
public class PaymentController : Controller
{
    private readonly IPublishEndpoint _publishEndpoint;

    public PaymentController(IPublishEndpoint publishEndpoint)
    {
        _publishEndpoint = publishEndpoint;
    }

    [HttpPost("/payment-callback")]
    public async Task<IActionResult> PaymentCallback([FromBody] PaymentSystemCallback paymentCallback)
    {
        var correlationId = NewId.NextGuid();

        await _publishEndpoint.Publish(paymentCallback, context => context.CorrelationId = correlationId);

        return Ok();
    }
}

public class CompletedCallback
{
    public Guid PaymentId { get; set; }
    public string CallbackUrl { get; set; }
}

public record OrderSagaCommand(Guid PaymentId, string CallbackUrl);


public class PaymentSystemCallback
{
    public Guid PaymentId { get; set; }
    public PaymentResult Result { get; set; }
}

public class PaymentTimeoutExpired
{
    public Guid PaymentId { get; set; }
}


public class OrderSagaState : SagaStateMachineInstance
{
    public Guid CorrelationId { get; set; }
    public string CurrentState { get; set; }
    public Guid PaymentId { get; set; }
    public string CallbackUrl { get; set; }
    public DateTime PaymentCompletedAt { get; set; }
    public PaymentResult PaymentResult { get; set; }
    public Guid? PaymentTimeoutToken { get; set; }
}

public class PaymentResult
{
    public bool IsSuccessful { get; set; }
    public string Message { get; set; }
}


public class OrderSagaStateMachine : MassTransitStateMachine<OrderSagaState>
{
    private readonly IConfiguration _configuration;

    public OrderSagaStateMachine(IConfiguration configuration)
    {
        _configuration = configuration;

        InstanceState(x => x.CurrentState);

        Event(() => PaymentSystemCallbackReceivedEvent,
            x => x.CorrelateById(context => context.Message.PaymentId));

        Event(() => CompletedCallbackReceivedEvent,
            x => x.CorrelateById(context => context.Message.PaymentId));

        Schedule(() => PaymentTimeoutExpired, state => state.PaymentTimeoutToken, s =>
        {
            var timeoutInSeconds = _configuration.GetValue<int>("PaymentTimeoutInSeconds");

            s.Delay = TimeSpan.FromSeconds(timeoutInSeconds);
        });

        Initially(
            When(PaymentSystemCallbackReceivedEvent)
                .Then(context =>
                {
                    context.Instance.PaymentId = context.Data.PaymentId;
                    context.Instance.CallbackUrl = context.Data.CallbackUrl;
                })
                .TransitionTo(WaitingForPayment)
                .Schedule(PaymentTimeoutExpired, context => context.Init<PaymentTimeoutExpired>(new
                {
                    PaymentId = context.Instance.PaymentId
                })),
            When(CompletedCallbackReceivedEvent)
                .Then(context =>
                {
                    context.Instance.PaymentResult = context.Data.Result;
                    context.Instance.PaymentCompletedAt = DateTime.UtcNow;
                })
                .TransitionTo(PaymentCompleted)
                .Finalize()
        );

        During(WaitingForPayment,
            When(PaymentTimeoutExpired.Received)
                .Then(context =>
                {
                    Console.WriteLine($"Payment timed out for {context.Instance.PaymentId}");
                })
                .Publish(context => new PaymentTimeoutExpired
                {
                    PaymentId = context.Instance.PaymentId
                })
                .TransitionTo(PaymentTimedOut));

        During(PaymentCompleted,
            Ignore(PaymentSystemCallbackReceivedEvent),
            Ignore(CompletedCallbackReceivedEvent));
    }

    public Event<PaymentSystemCallback> PaymentSystemCallbackReceivedEvent { get; private set; }
    public Event<CompletedCallback> CompletedCallbackReceivedEvent { get; private set; }

    public Schedule<OrderSagaState, PaymentTimeoutExpired> PaymentTimeoutExpired { get; private set; }

    public State WaitingForPayment { get; private set; }
    public State PaymentCompleted { get; private set; }
    public State PaymentTimedOut { get; private set; }
}


