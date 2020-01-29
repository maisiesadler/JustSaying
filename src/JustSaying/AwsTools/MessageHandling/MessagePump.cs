using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SQS.Model;
using Microsoft.Extensions.Logging;

namespace JustSaying.AwsTools.MessageHandling
{
    public class MessagePump
    {
        private readonly IAsyncEnumerable<(Message, IMessageDispatcher)> _messages;
        private readonly ILogger _logger;
        public int Id { get; set; }

        public MessagePump(
            IAsyncEnumerable<(Message, IMessageDispatcher)> messages,
            ILogger log)
        {
            _messages = messages;
            _logger = log;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            return Task.Run(async () => { await EventLoop(cancellationToken).ConfigureAwait(false); });
        }

        private async Task EventLoop(CancellationToken cancellationToken)
        {
            // todo: exception handling
            await foreach ((Message message, IMessageDispatcher messageDispatcher) in _messages)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    // ?
                }
                _logger.LogInformation($"{Id} got: {message}");
                await messageDispatcher.DispatchMessage(message, CancellationToken.None).ConfigureAwait(false);
            }

            _logger.LogInformation($"{Id} Loop completed.");
        }
    }
}
