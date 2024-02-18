using Microsoft.Extensions.Logging;

interface IHandler
{
    TimeSpan Timeout { get; }

    Task PerformOperation(CancellationToken cancellationToken);
}

class Handler : IHandler
{
    private readonly IConsumer _consumer;
    private readonly IPublisher _publisher;
    private readonly ILogger<Handler> _logger;

    public TimeSpan Timeout { get; }

    public Handler(
      TimeSpan timeout,
      IConsumer consumer,
      IPublisher publisher,
      ILogger<Handler> logger)
    {
        Timeout = timeout;

        _consumer = consumer;
        _publisher = publisher;
        _logger = logger;
    }

    public async Task PerformOperation(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var result = _consumer.ReadData();

            if (result.Result == null)
            {
                _logger.LogDebug($"No items");

                await Task.Delay(Timeout, cancellationToken);

                continue;
            }

            var payload = result?.Result.Payload;
            var recipients = result?.Result.Recipients;

            if (payload != null && recipients != null)
            {
                foreach (var address in recipients)
                {
                    var sendResult = await _publisher.SendData(address, payload);

                    if (sendResult == SendResult.Rejected)
                    {
                        _logger.LogDebug($"Data is sent successfully");

                        await Task.Delay(Timeout, cancellationToken);
                    }
                    else
                    {
                        _logger.LogDebug($"Data is sent successfully");
                    }
                }
            }
        }

        await Task.CompletedTask;
    }
}

record Payload(string Origin, byte[] Data);
record Address(string DataCenter, string NodeId);
record Event(IReadOnlyCollection<Address> Recipients, Payload Payload);

enum SendResult
{
    Accepted,
    Rejected
}

interface IConsumer
{
    Task<Event> ReadData();
}

interface IPublisher
{
    Task<SendResult> SendData(Address address, Payload payload);
}
