using Confluent.Kafka;
using MongoDB.Driver;
using MongoDB.Bson;
using System.Globalization;

namespace Consumer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            string currentDirectory = Directory.GetCurrentDirectory();
            string configFile = Path.Combine(currentDirectory, "..", "config.ini");
            var consumerIniConfigManager = new ConsumerIniConfigManager(configFile);

            var config = new ConsumerConfig
            {
                BootstrapServers = consumerIniConfigManager.KafkaServer,
                GroupId = consumerIniConfigManager.GroupId,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var client = new MongoClient(consumerIniConfigManager.MongoConnectionString);
            var database = client.GetDatabase(consumerIniConfigManager.MongoDatabaseName);
            var collection = database.GetCollection<BsonDocument>(consumerIniConfigManager.CollectionName);

            using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
            {
                consumer.Subscribe(consumerIniConfigManager.TopicName);
                Console.WriteLine("Consumer is running...");
                try
                {
                    while (true)
                    {
                        Console.WriteLine("Proccessing new messages...");
                        var consumeResult = consumer.Consume(TimeSpan.FromSeconds(consumerIniConfigManager.ConsumptionInterval));

                        if (consumeResult != null)
                        {
                            Console.WriteLine($"--Received message: {consumeResult.Message.Value}--");

                            var document = MongoDB.Bson.Serialization.BsonSerializer.Deserialize<BsonDocument>(consumeResult.Message.Value);
                            string timestampString = document["Timestamp"].AsString;

                            string formatString = "yyyy-MM-dd'T'HH:mm:ss.FFFFFFF'Z'";
                            DateTime timestamp;

                            if (timestampString.Length == 24) 
                            {
                                formatString = "yyyy-MM-dd'T'HH:mm:ss.FFFFFF'Z'";
                            }

                            timestamp = DateTime.ParseExact(
                                timestampString, 
                                formatString, CultureInfo.InvariantCulture, 
                                DateTimeStyles.RoundtripKind
                            );

                            document["Timestamp"] = BsonDateTime.Create(timestamp);

                            await collection.InsertOneAsync(document);
                            Console.WriteLine("--Saved to database--");
        
                            consumer.Commit(consumeResult);
                        }
                        else
                        {
                            Console.WriteLine("--No new messages to proccess.--");
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
                finally
                {
                    consumer.Close();
                }
            }
        }
    }
}
