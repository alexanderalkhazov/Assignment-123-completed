using Confluent.Kafka;
using MongoDB.Driver;
using MongoDB.Bson;
using IniParser;
using IniParser.Model;
using System.Globalization;
using System.Data;

namespace Consumer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            string currentDirectory = Directory.GetCurrentDirectory();
            string configFile = Path.Combine(currentDirectory, "..", "config.ini");
            var parser = new FileIniDataParser();
            IniData fileContext = parser.ReadFile(configFile);

            string kafkaServer = fileContext["KAFKA"]["KAFKA_SERVER"];
            string topicName = fileContext["KAFKA"]["KAFKA_TOPIC_NAME"];
            string groupId = fileContext["KAFKA"]["GROUP_ID"];

            string mongoConnectionString = fileContext["MONGO_DB"]["MONGO_CONNECTION_STRING"];
            string mongoDatabaseName = fileContext["MONGO_DB"]["DATABASE_NAME"];
            string collectionName = fileContext["MONGO_DB"]["COLLECTION_NAME"];
            int consumptionInterval = int.Parse(fileContext["EVENT_DETAILS"]["CONSUMPTION_INTERVAL"]);

            if (
                string.IsNullOrEmpty(kafkaServer) ||
                string.IsNullOrEmpty(mongoConnectionString) ||
                string.IsNullOrEmpty(mongoDatabaseName) ||
                string.IsNullOrEmpty(collectionName) ||
                string.IsNullOrEmpty(topicName) ||
                string.IsNullOrEmpty(groupId)
            )
            {
                Console.WriteLine("One or more environment variables are missing.");
                return;
            }

            var config = new ConsumerConfig
            {
                BootstrapServers = kafkaServer,
                GroupId = groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var client = new MongoClient(mongoConnectionString);
            var database = client.GetDatabase(mongoDatabaseName);
            var collection = database.GetCollection<BsonDocument>(collectionName);

            using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
            {
                consumer.Subscribe(topicName);
                Console.WriteLine("Consumer is running...");
                try
                {
                    while (true)
                    {
                        Console.WriteLine("Proccessing new messages...");
                        var consumeResult = consumer.Consume(TimeSpan.FromSeconds(consumptionInterval));

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

                            timestamp = DateTime.ParseExact(timestampString, formatString, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);

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
                    Console.WriteLine($"Error: {ex.Message}");
                }
                finally
                {
                    consumer.Close();
                }
            }
        }
    }
}
