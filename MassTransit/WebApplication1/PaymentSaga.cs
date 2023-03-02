using System;
using System.Net;
using System.Threading.Tasks;
using Dapper;
using MassTransit;
using Microsoft.Extensions.Logging;
using Polly;
using RestSharp;

namespace PaymentService.Sagas
{
    public class PaymentSaga : ISaga, 
                                InitiatedBy<PaymentSystemCallback>, 
                                Orchestrates<CompletedCallback>
    {
        private readonly ILogger<PaymentSaga> _logger;
        private readonly IDbConnection _connection;
        private readonly IRequestClient<ProcessPayment> _processPaymentClient;
        
        private PaymentSystemCallback _paymentSystemCallback;

        public Guid CorrelationId { get; set; }
        public async Task Consume(ConsumeContext<PaymentSystemCallback> context)
        {
            _logger.LogInformation("PaymentSystemCallback received with OrderId: {OrderId}", context.Message.OrderId);
            _paymentSystemCallback = context.Message;
            await _connection.ExecuteAsync(
                "INSERT INTO PaymentSaga (CorrelationId, OrderId, Body) VALUES (@CorrelationId, @OrderId, @Body)",
                new
                {
                    CorrelationId = CorrelationId,
                    OrderId = _paymentSystemCallback.OrderId,
                    Body = _paymentSystemCallback.Body
                }
            );

            await context.Publish<HttpBodyStoredEvent>(new
            {
                CorrelationId = CorrelationId,
                OrderId = _paymentSystemCallback.OrderId
            });
        }

        public async Task Consume(ConsumeContext<CompletedCallback> context)
        {
            _logger.LogInformation("CompletedCallback received with OrderId: {OrderId}", context.Message.OrderId);
            var processPayment = new ProcessPayment()
            {
                OrderId = context.Message.OrderId,
                Amount = 100 // TODO: Retrieve amount from the stored HttpBody
            };

            var retryPolicy = Policy
                .Handle<Exception>()
                .WaitAndRetryAsync(3, i => TimeSpan.FromSeconds(10), (ex, ts) => _logger.LogError(ex, "Error sending request to external web service"));

            var restClient = new RestClient("https://external-web-service.com");
            var restRequest = new RestRequest("processPayment", Method.POST);
            restRequest.AddJsonBody(processPayment);

            await retryPolicy.ExecuteAsync(async () =>
            {
                var response = await restClient.ExecuteAsync(restRequest);
                if (response.StatusCode != HttpStatusCode.OK)
                {
                    _logger.LogError("Error sending request to external web service. StatusCode: {StatusCode}, Content: {Content}", response.StatusCode, response.Content);
                    throw new Exception($"Error sending request to external web service. StatusCode: {response.StatusCode}");
                }

                _logger.LogInformation("Request sent to external web service successfully. Response: {Content}", response.Content);
            });

            await context.MarkAsComplete();
        }

        public PaymentSaga(ILogger<PaymentSaga> logger, IDbConnection connection, IRequestClient<ProcessPayment> processPaymentClient)
        {
            _logger = logger;
            _connection = connection;
            _processPaymentClient = processPaymentClient;
        }

        public PaymentSaga()
        {
        }
    }
}
