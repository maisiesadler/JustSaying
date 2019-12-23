using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JustSaying.AwsTools.MessageHandling;
using JustSaying.Messaging;
using JustSaying.Models;
using Xunit;
using Xunit.Abstractions;

namespace JustSaying.UnitTests.AwsTools.MessageHandling.SqsNotificationListener2
{
    public class ConfigurationTests
    {
        private readonly ITestOutputHelper _testOutputHelper;

        public ConfigurationTests(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
        }

        [Fact]
        public async Task QueueCanBeAssignedToOnePump()
        {
            var producer = new Producer("one");
            var buffer = new MessageChannelBuffer(10, () => producer.GetNext());
            var cts = new CancellationTokenSource();
            cts.CancelAfter(TimeSpan.FromSeconds(2));

            var t1 = Listen(buffer.Messages(), "one");

            await buffer.BeginProducingAsync(cts.Token);

            await Task.WhenAll(t1);
        }

        [Fact]
        public async Task QueueCanBeAssignedToMultiplePumps()
        {
            var producer = new Producer("one");
            var buffer = new MessageChannelBuffer(10, () => producer.GetNext());
            var cts = new CancellationTokenSource();
            cts.CancelAfter(TimeSpan.FromSeconds(2));

            var messagePumpCollection = new MessagePumpCollection(3, _testOutputHelper.ToLogger<MessagePumpCollection>());

            var t = messagePumpCollection.Listen(buffer, cts.Token);

            messagePumpCollection.StartAsync();
            await buffer.BeginProducingAsync(cts.Token);

            await t;
        }

        [Fact]
        public async Task MultipleQueuesCanBeAssignedToOnePump()
        {
            var producer = new Producer("one");
            var buffer = new MessageChannelBuffer(10, () => producer.GetNext());
            var producer2 = new Producer("two");
            var buffer2 = new MessageChannelBuffer(10, () => producer2.GetNext());
            var cts = new CancellationTokenSource();
            cts.CancelAfter(TimeSpan.FromSeconds(2));

            var messagePumpCollection = new MessagePumpCollection(3, _testOutputHelper.ToLogger<MessagePumpCollection>());

            var t1 = messagePumpCollection.Listen(buffer, cts.Token);
            var t2 = messagePumpCollection.Listen(buffer, cts.Token);

            messagePumpCollection.StartAsync();
            await buffer.BeginProducingAsync(cts.Token);
            await buffer2.BeginProducingAsync(cts.Token);

            await t1;
            await t2;
        }

        private async Task Listen(IAsyncEnumerable<Message> messages, string prefix)
        {
            await foreach (var msg in messages)
            {
                _testOutputHelper.WriteLine($"{prefix}-{msg.Tenant}");
                await Task.Delay(5).ConfigureAwait(false);
            }
        }

        private class Producer
        {
            private readonly string _prefix;
            private int _id;
            public Producer(string prefix)
            {
                _prefix = prefix;
            }

            public TestMessage GetNext()
            {
                var id = Interlocked.Increment(ref _id);
                return new TestMessage { Tenant = _prefix + "-" + id.ToString(CultureInfo.InvariantCulture) };
            }
        }

        private class TestMessage : Message
        {
            public override string ToString()
            {
                return Tenant;
            }
        }
    }
}
