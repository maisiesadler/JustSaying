using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using JustSaying.Models;

namespace JustSaying.AwsTools.MessageHandling
{
    public interface IMessageBuffer
    {
        string QueueName { get; }
        Task BeginProducingAsync(CancellationToken cancellationToken);
        IAsyncEnumerable<Message> Messages();
    }
}
