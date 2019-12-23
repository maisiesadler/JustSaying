using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using JustSaying.Messaging;
using JustSaying.Messaging.Interrogation;
using JustSaying.Messaging.MessageHandling;
using Microsoft.Extensions.Logging;
using Message = JustSaying.Models.Message;

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

        public void AddMessageHandler<T>(Func<IHandlerAsync<T>> handler) where T : Message
        {
            throw new NotImplementedException();
        }

        public void Listen(CancellationToken cancellationToken)
        {
            _messagePumpCollection.Listen(_messageBuffer, cancellationToken);
        }
    }

    public interface IMessagePumpCollection
    {
        Task Listen(IMessageBuffer messageBuffer, CancellationToken cancellationToken);
    }

    public class MessagePumpCollection : IMessagePumpCollection
    {
        private readonly Channel<Message> _channel;
        private readonly int _numberOfPumps;
        private readonly ILogger _logger;

        public MessagePumpCollection(int numberOfPumps, ILogger logger)
        {
            _channel = Channel.CreateUnbounded<Message>();
            _numberOfPumps = numberOfPumps;
            _logger = logger;
        }

        public async Task Listen(IMessageBuffer messageBuffer, CancellationToken cancellationToken)
        {
            await foreach (var message in messageBuffer.Messages())
            {
                await _channel.Writer.WriteAsync(message);
            }
        }

        public void StartAsync()
        {
            for (var i = 0; i < _numberOfPumps; i++)
            {
                var ii = i;
                _ = Task.Run(async () =>
                {
                    await foreach (var msg in Messages())
                    {
                        _logger.LogInformation($"{ii} got: {msg}");
                    }
                });
            }
        }

        private async IAsyncEnumerable<Message> Messages()
        {
            while (await _channel.Reader.WaitToReadAsync())
            {
                yield return await _channel.Reader.ReadAsync();
            }
        }
    }
}
