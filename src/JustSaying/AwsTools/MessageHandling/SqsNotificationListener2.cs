using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using JustSaying.Messaging;
using JustSaying.Messaging.Interrogation;
using JustSaying.Messaging.MessageHandling;
using Microsoft.Extensions.Logging;
using Message = Amazon.SQS.Model.Message;

namespace JustSaying.AwsTools.MessageHandling
{
    public class SqsNotificationListener2 : INotificationSubscriber
    {
        private readonly IMessageBuffer _messageBuffer;
        private readonly IMessageDispatcher _messageDispatcher;
        private readonly IMessagePumpCollection _messagePumpCollection;

        public bool IsListening { get; }

        public string Queue { get; }

        public ICollection<ISubscriber> Subscribers { get; }

        public SqsNotificationListener2(
            IMessageBuffer messageBuffer,
            IMessageDispatcher messageDispatcher,
            IMessagePumpCollection messagePumpCollection)
        {
            _messageBuffer = messageBuffer;
            _messageDispatcher = messageDispatcher;
            _messagePumpCollection = messagePumpCollection;
        }

        public void AddMessageHandler<T>(Func<IHandlerAsync<T>> handler) where T : JustSaying.Models.Message
        {
            throw new NotImplementedException();
        }

        public void Listen(CancellationToken cancellationToken)
        {
            _messagePumpCollection.Listen(_messageBuffer, _messageDispatcher, cancellationToken);
        }
    }

    public interface IMessagePumpCollection
    {
        Task Listen(IMessageBuffer messageBuffer, IMessageDispatcher messageDispatcher, CancellationToken cancellationToken);
    }

    public class MessagePumpCollection : IMessagePumpCollection
    {
        private readonly Channel<(Message, IMessageDispatcher)> _channel;
        private readonly int _numberOfPumps;
        private readonly ILogger _logger;

        public MessagePumpCollection(int numberOfPumps, ILogger logger)
        {
            _channel = Channel.CreateUnbounded<(Message, IMessageDispatcher)>();
            _numberOfPumps = numberOfPumps;
            _logger = logger;
        }

        public async Task Listen(IMessageBuffer messageBuffer, IMessageDispatcher messageDispatcher, CancellationToken cancellationToken)
        {
            await foreach (var message in messageBuffer.Messages())
            {
                await _channel.Writer.WriteAsync((message, messageDispatcher));
            }
        }

        public void StartAsync(CancellationToken cancellationToken)
        {
            for (var i = 0; i < _numberOfPumps; i++)
            {
                var ii = i;
                _ = Task.Run(async () =>
                {
                    await foreach ((Message message, IMessageDispatcher messageDispatcher) in Messages())
                    {
                        _logger.LogInformation($"{ii} got: {message}");
                        await messageDispatcher.DispatchMessage(message, cancellationToken).ConfigureAwait(false);
                    }
                });
            }
        }

        private async IAsyncEnumerable<(Message, IMessageDispatcher)> Messages()
        {
            while (await _channel.Reader.WaitToReadAsync())
            {
                yield return await _channel.Reader.ReadAsync();
            }
        }
    }
}
