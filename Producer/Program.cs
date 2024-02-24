using Confluent.Kafka;
using Newtonsoft.Json;

internal class Program
{
    private static async Task Main(string[] args)
    {
        string currentDirectory = Directory.GetCurrentDirectory();
        
        string configFile = Path.Combine(currentDirectory, "..", "config.ini");

        var producerIniConfigManager = new ProducerIniConfigManager(configFile);
        var eventFactory = new EventEntityFactory(producerIniConfigManager);


        var config = new ProducerConfig { BootstrapServers = producerIniConfigManager.KafkaServer };
        using (var producer = new ProducerBuilder<Null, string>(config).Build())
        {
            Console.WriteLine("Producer is running...");
            try
            {
                while (true)
                {
                    var eventEntity = eventFactory.CreateEventEntity();
                    string json = JsonConvert.SerializeObject(eventEntity);
                    await producer.ProduceAsync(producerIniConfigManager.TopicName, new Message<Null, string> { Value = json });
                    Console.WriteLine($"--Produced message: {json} to topic {producerIniConfigManager.TopicName}--");
                    await Task.Delay(TimeSpan.FromSeconds(producerIniConfigManager.ProductionInterval));
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
    }
}