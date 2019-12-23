using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using JustSaying.Models;

namespace JustSaying.AwsTools.MessageHandling
{
    public class MessageChannelBuffer : IMessageBuffer
    {
        public string QueueName { get; }
        private readonly Channel<Message> _channel;
        private readonly Func<Message> _getMsg;

        public MessageChannelBuffer(int bufferLength, Func<Message> getMsg)
        {
            _channel = Channel.CreateBounded<Message>(bufferLength);
            _getMsg = getMsg;
        }

        public async Task BeginProducingAsync(CancellationToken cancellationToken)
        {
            ChannelWriter<Message> writer = _channel.Writer;
            while (!cancellationToken.IsCancellationRequested)
            {
                bool writePermitted = await writer.WaitToWriteAsync(cancellationToken);
                if (!writePermitted)
                {
                    break;
                }

                var messages = await GetMessagesFromSqs().ConfigureAwait(false);
                foreach (var message in messages)
                {
                    await writer.WriteAsync(message).ConfigureAwait(false);
                }
            }

            writer.Complete();
        }

        public async IAsyncEnumerable<Message> Messages()
        {
            while (await _channel.Reader.WaitToReadAsync())
            {
                while (_channel.Reader.TryRead(out Message message))
                {
                    yield return message;
                }
            }
        }

        private async Task<IList<Message>> GetMessagesFromSqs()
        {
            await Task.Delay(100).ConfigureAwait(false);
            IList<Message> msgs = new List<Message> { _getMsg() };
            return msgs;
        }
    }
}
