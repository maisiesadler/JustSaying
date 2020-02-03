using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SQS.Model;
using JustSaying.AwsTools.MessageHandling;
using JustSaying.Messaging;
using Microsoft.Extensions.Logging;
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
            var handlerMap = new HandlerMap();

            var producer = new Producer("one");
            var buffer = new MessageChannelBuffer(10, () => producer.GetNext());
            var cts = new CancellationTokenSource();
            cts.CancelAfter(TimeSpan.FromSeconds(2));
            var dispatcher = new LoggingDispatcher(_testOutputHelper.ToLogger<LoggingDispatcher>(), "one", handlerMap);

            var messagePumpCollection = new MessagePumpCollection(3, dispatcher, _testOutputHelper.ToLogger<MessagePumpCollection>());

            var pumpTask = messagePumpCollection.Listen(buffer, cts.Token);

            var writeTask = buffer.BeginProducingAsync(cts.Token);

            messagePumpCollection.StartAsync(CancellationToken.None);

            await writeTask;
            await pumpTask;
        }

        [Fact]
        public async Task MultipleQueuesCanBeAssignedToOnePump()
        {
            var handlerMap = new HandlerMap();

            var producer = new Producer("one");
            var buffer = new MessageChannelBuffer(10, () => producer.GetNext());
            var producer2 = new Producer("two");
            var buffer2 = new MessageChannelBuffer(10, () => producer2.GetNext());
            var cts = new CancellationTokenSource();
            cts.CancelAfter(TimeSpan.FromSeconds(2));

            // dispatcher configured to handle all messages using handlerMap
            var dispatcher = new LoggingDispatcher(_testOutputHelper.ToLogger<LoggingDispatcher>(), "one", handlerMap);

            var messagePumpCollection = new MessagePumpCollection(3, dispatcher, _testOutputHelper.ToLogger<MessagePumpCollection>());

            var pumpTask1 = messagePumpCollection.Listen(buffer, cts.Token);
            var pumpTask2 = messagePumpCollection.Listen(buffer2, cts.Token);

            var writeTask1 = buffer.BeginProducingAsync(cts.Token);
            var writeTask2 = buffer2.BeginProducingAsync(cts.Token);

            messagePumpCollection.StartAsync(CancellationToken.None);

            await writeTask1;
            await writeTask2;
            await pumpTask1;
            await pumpTask2;
        }

        [Fact]
        public async Task TestConfiguration()
        {
            var handlerMap = new HandlerMap();

            var producer = new Producer("one");
            var buffer = new MessageChannelBuffer(10, () => producer.GetNext());
            var producer2 = new Producer("two");
            var buffer2 = new MessageChannelBuffer(10, () => producer2.GetNext());
            var cts = new CancellationTokenSource();
            cts.CancelAfter(TimeSpan.FromSeconds(2));
            var dispatcher = new LoggingDispatcher(_testOutputHelper.ToLogger<LoggingDispatcher>(), "one", handlerMap);

            var messagePumpCollection = new MessagePumpCollection(3, dispatcher, _testOutputHelper.ToLogger<MessagePumpCollection>());

            var pumpTask1 = messagePumpCollection.Listen(buffer, cts.Token);
            var pumpTask2 = messagePumpCollection.Listen(buffer2, cts.Token);

            var writeTask1 = buffer.BeginProducingAsync(cts.Token);
            var writeTask2 = buffer2.BeginProducingAsync(cts.Token);

            messagePumpCollection.StartAsync(CancellationToken.None);

            await writeTask1;
            await writeTask2;
            await pumpTask1;
            await pumpTask2;
        }

        private async Task Listen(IAsyncEnumerable<Message> messages, string prefix)
        {
            await foreach (var msg in messages)
            {
                _testOutputHelper.WriteLine($"{prefix}-{msg.Body}");
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
                return new TestMessage { Body = _prefix + "-" + id.ToString(CultureInfo.InvariantCulture) };
            }
        }

        private class LoggingDispatcher : IMessageDispatcher
        {
            private readonly ILogger _logger;
            private readonly string _prefix;
            private readonly HandlerMap _handlerMap;

            public LoggingDispatcher(
                ILogger logger,
                string prefix,
                HandlerMap handlerMap)
            {
                _logger = logger;
                _prefix = prefix;
                _handlerMap = handlerMap;
            }

            public Task DispatchMessage(Message message, CancellationToken cancellationToken)
            {
                // get message type
                // find type in HandlerMap

                _logger.LogInformation($"Dispatcher {_prefix} got message '{message}'");
                return Task.CompletedTask;
            }
        }

        private class TestMessage : Message
        {
            public override string ToString()
            {
                return Body;
            }
        }
    }
}
