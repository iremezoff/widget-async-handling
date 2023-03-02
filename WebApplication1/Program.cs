using System.Security.Cryptography;
using System.Text;
using Dapper;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using PaymentSystemDemo.Data;
using PaymentSystemDemo.Services;
using Polly;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddScoped<IPaymentRepository, PaymentRepository>();
builder.Services.AddScoped<IPaymentService, PaymentService>();
builder.Services.AddSingleton<IExternalPaymentService, ExternalPaymentService>();
builder.Services.AddSingleton<IHashService, Sha256HashService>();

builder.Services.AddHostedService<TimedHostedService>();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
  app.UseDeveloperExceptionPage();
}

app.UseHttpsRedirection();

app.UseRouting();

app.UseAuthorization();

app.MapControllers();

app.Run();


public class PaymentSystemCallback
{
    public int Id { get; set; }
    public string OrderId { get; set; }
    public string TransactionId { get; set; }
    public decimal Amount { get; set; }
    public PaymentSystemCallbackStatus Status { get; set; }
    public DateTime ReceivedOn { get; set; }
    public string Signature { get; set; }
}

public enum PaymentSystemCallbackStatus
{
    Received,
    Processed,
    Failed
}

public interface IPaymentRepository
{
    Task AddPaymentCallbackAsync(PaymentSystemCallback paymentCallback);
    Task<IEnumerable<PaymentSystemCallback>> GetPendingCallbacksAsync();
    Task SetCallbackProcessedAsync(int id);
}

public class PaymentRepository : IPaymentRepository
{
    private readonly IConfiguration _configuration;

    public PaymentRepository(IConfiguration configuration)
    {
        _configuration = configuration;
    }

    public async Task AddPaymentCallbackAsync(PaymentSystemCallback paymentCallback)
    {
        using var connection = new SqlConnection(_configuration.GetConnectionString("DefaultConnection"));
        await connection.OpenAsync();
        await connection.ExecuteAsync(
            "INSERT INTO PaymentSystemCallbacks (Status, Amount, Currency, Signature, Timestamp) " +
            "VALUES (@Status, @Amount, @Currency, @Signature, @Timestamp)",
            paymentCallback);
    }

    public async Task<IEnumerable<PaymentSystemCallback>> GetPendingCallbacksAsync()
    {
        using var connection = new SqlConnection(_configuration.GetConnectionString("DefaultConnection"));
        await connection.OpenAsync();
        return await connection.QueryAsync<PaymentSystemCallback>(
            "SELECT * FROM PaymentSystemCallbacks WHERE Status = 'Pending'");
    }

    public async Task SetCallbackProcessedAsync(int id)
    {
        using var connection = new SqlConnection(_configuration.GetConnectionString("DefaultConnection"));
        await connection.OpenAsync();
        await connection.ExecuteAsync(
            "UPDATE PaymentSystemCallbacks SET Status = 'Processed' WHERE Id = @Id",
            new { Id = id });
    }
}

public interface IPaymentService
{
    Task<PaymentSystemResponse> ProcessPaymentAsync(PaymentSystemRequest request);
}

public class TimedHostedService : IHostedService, IDisposable
{
    private readonly ILogger<TimedHostedService> _logger;
    private readonly IServiceProvider _services;
    private Timer _timer;

    public TimedHostedService(ILogger<TimedHostedService> logger, IServiceProvider services)
    {
        _logger = logger;
        _services = services;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Timed Hosted Service running.");

        _timer = new Timer(DoWork, null, TimeSpan.Zero, TimeSpan.FromSeconds(10));

        return Task.CompletedTask;
    }

    private async void DoWork(object state)
    {
        using (var scope = _services.CreateScope())
        {
            var paymentRepository = scope.ServiceProvider.GetRequiredService<IPaymentRepository>();

            var payment = await paymentRepository.GetNextPendingPayment();

            if (payment != null)
            {
                var paymentService = scope.ServiceProvider.GetRequiredService<IPaymentService>();
                var policy = Policy
                    .Handle<Exception>()
                    .WaitAndRetryAsync(3, retryAttempt => TimeSpan.FromSeconds(10));

                try
                {
                    await policy.ExecuteAsync(async () =>
                    {
                        await paymentService.ProcessPayment(payment);
                        await paymentRepository.SetPaymentState(payment.Id, PaymentState.Completed);
                    });
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing payment {PaymentId}", payment.Id);
                    await paymentRepository.SetPaymentState(payment.Id, PaymentState.Failed);
                }
            }
        }
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Timed Hosted Service is stopping.");

        _timer?.Change(Timeout.Infinite, 0);

        return Task.CompletedTask;
    }

    public void Dispose()
    {
        _timer?.Dispose();
    }
}

public class PaymentService : IPaymentService
{
    private readonly HttpClient _httpClient;
    private readonly ILogger<PaymentService> _logger;
    private readonly string _apiUrl;

    public PaymentService(HttpClient httpClient, IConfiguration configuration, ILogger<PaymentService> logger)
    {
        _httpClient = httpClient;
        _logger = logger;
        _apiUrl = configuration.GetValue<string>("PaymentSystemApiUrl");
    }

    public async Task<PaymentSystemResponse> ProcessPaymentAsync(PaymentSystemRequest request)
    {
        // Serialize the request object to JSON
        var requestJson = JsonConvert.SerializeObject(request);

        // Calculate the request signature
        var signature = GetRequestSignature(request);

        // Create the HTTP request message
        var httpRequest = new HttpRequestMessage(HttpMethod.Post, _apiUrl);
        httpRequest.Headers.Add("X-Signature", signature);
        httpRequest.Content = new StringContent(requestJson, Encoding.UTF8, "application/json");

        // Send the HTTP request and get the response
        var httpResponse = await _httpClient.SendAsync(httpRequest);

        // Check if the response is successful
        if (httpResponse.IsSuccessStatusCode)
        {
            // Deserialize the response content to PaymentSystemResponse object
            var responseJson = await httpResponse.Content.ReadAsStringAsync();
            var response = JsonConvert.DeserializeObject<PaymentSystemResponse>(responseJson);

            return response;
        }
        else
        {
            // Log the error and throw an exception
            var errorJson = await httpResponse.Content.ReadAsStringAsync();
            _logger.LogError("Failed to process payment. Response status code: {StatusCode}. Response content: {ResponseContent}.", httpResponse.StatusCode, errorJson);
            throw new Exception($"Failed to process payment. Response status code: {httpResponse.StatusCode}. Response content: {errorJson}.");
        }
    }

    private string GetRequestSignature(PaymentSystemRequest request)
    {
        // Concatenate all the fields in the request object
        var requestString = $"{request.OrderId}{request.Amount}{request.Currency}{request.CardNumber}{request.CardHolderName}{request.ExpirationMonth}{request.ExpirationYear}{request.Cvv}";

        // Calculate the SHA256 hash of the request string
        var sha256 = SHA256.Create();
        var hashBytes = sha256.ComputeHash(Encoding.UTF8.GetBytes(requestString));

        // Convert the hash bytes to a hex string
        var hexString = BitConverter.ToString(hashBytes).Replace("-", string.Empty);

        return hexString;
    }
}

public class PaymentSystemRequest
{
    public string TransactionId { get; set; }
    public string AccountNumber { get; set; }
    public decimal Amount { get; set; }
    public string Currency { get; set; }
    public string Signature { get; set; }
}

public class PaymentSystemResponse
{
    public string TransactionId { get; set; }
    public string Status { get; set; }
    public string ErrorMessage { get; set; }
}



