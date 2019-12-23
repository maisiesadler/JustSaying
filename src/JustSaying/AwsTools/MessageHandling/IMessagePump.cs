using System.Threading.Tasks;

namespace JustSaying.AwsTools.MessageHandling
{
    public interface IMessagePump
    {
        Task StartAsync();
    }
}
