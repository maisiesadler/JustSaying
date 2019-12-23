using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SQS.Model;

namespace JustSaying.AwsTools.MessageHandling
{
    public interface IMessageBuffer
    {
        string QueueName { get; }
        Task BeginProducingAsync(CancellationToken cancellationToken);
        IAsyncEnumerable<Message> Messages();
    }
}
