using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace WorkerService
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;

        public Worker(ILogger<Worker> logger)
        {
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            const string topic = "testbatch";
            var consumer = BuildConsumer();
            consumer.Subscribe(topic);

            //create my wrapper that will control batch consume in c#
            var consumerBatch = KafkaConsumerBatchBuilder<string, string>.Config()
                .WithConsumer(consumer)
                .WithLogger(_logger)
                .WithBatchSize(50)
                .WithMaxWaitTime(TimeSpan.FromSeconds(10))
                .Build();
            
            while (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation("Consuming from topic {}", topic);
                try
                {
                    var results = consumerBatch.ConsumeBatch();
                    var consumeResults = results.ToList();
                    _logger.LogInformation("Consumed {} results", consumeResults.Count);
                    if (consumeResults.Count > 0)
                    {
                        _logger.LogInformation("Commit");
                        consumer.Commit();
                     
                        _logger.LogInformation("SeekBach");
                       consumerBatch.SeekBatch();
                       Thread.Sleep(TimeSpan.FromSeconds(20));
                        
                    }
                    
                }
                catch (ConsumeException e)
                {
                    _logger.LogError(e, "Error consuming message on topic {}", topic);
                    break;
                }
                
                await Task.Delay(1000, stoppingToken);
            }
        }

        private IConsumer<string, string> BuildConsumer()
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "testbatch",
                EnableAutoCommit = false
            };
            return new ConsumerBuilder<string,string>(config).Build();
        }
    }
}