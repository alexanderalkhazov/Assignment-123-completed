using MongoDB.Driver;
using StackExchange.Redis;
using MongoDB.Bson;
using System.Globalization;

namespace ETL
{
    class Program
    {
        static async Task Main(string[] args)
        {
            string currentDirectory = Directory.GetCurrentDirectory();
            string configFile = Path.Combine(currentDirectory, "..", "config.ini");
            var etlIniConfigManager = new ETLIniConfigManager(configFile);

            Console.WriteLine("ETL proccess is running.");

            try
            {
                var mongoClient = new MongoClient(etlIniConfigManager.MongoConnectionString);
                var mongoDatabase = mongoClient.GetDatabase(etlIniConfigManager.MongoDatabaseName);
                var mongoCollection = mongoDatabase.GetCollection<BsonDocument>(etlIniConfigManager.CollectionName);
                var redis = ConnectionMultiplexer.Connect(etlIniConfigManager.RedisServerString!);
                var redisDatabase = redis.GetDatabase();

                while (true)
                {
                    Console.WriteLine("ETL processing...");
                    await ProccessEventsAsync(mongoCollection, redisDatabase);
                    Console.WriteLine("Cool down...");
                    Thread.Sleep(etlIniConfigManager.EtlSleepDuration);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        private static async Task ProccessEventsAsync(IMongoCollection<BsonDocument> mongoCollection, IDatabase redisDatabase)
        {
            string? latestTimestamp = GetLatestTimestamp(redisDatabase);
            var filter = GetCorrectFilterDefinition(latestTimestamp);
            var newEvents = await mongoCollection.Find(filter).ToListAsync();
            await SaveAllEventsAsync(newEvents, redisDatabase);
        }

        private static FilterDefinition<BsonDocument> GetCorrectFilterDefinition(string? latestTimestamp) 
        {
            if (!string.IsNullOrEmpty(latestTimestamp))
            {
                DateTime latestDateTimeTimestamp = DateTime.Parse(latestTimestamp, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
                return Builders<BsonDocument>.Filter.Gt("Timestamp", latestDateTimeTimestamp);
            } 
            return Builders<BsonDocument>.Filter.Empty;
        }

        private static async Task SaveAllEventsAsync(List<BsonDocument>? newEvents, IDatabase redisDatabase)
        {
            if (newEvents == null || newEvents.Count == 0)
            {
                Console.WriteLine("No new Events!");
                return;
            }
            foreach (var eventEntity in newEvents)
            {
                await SaveEventEntityAndUpdateTimestampAsync(eventEntity, redisDatabase);
            }
        }

        private static async Task SaveEventEntityAndUpdateTimestampAsync(BsonDocument eventEntity, IDatabase redisDatabase)
        {
            Console.WriteLine($"new eventObj: {eventEntity["Timestamp"]}");
            var timestamp = ConvertToTimestamp(eventEntity);
            var key = $"{eventEntity["ReporterId"].AsInt32}:{eventEntity["Timestamp"].AsBsonDateTime}";

            var keyValues = new List<KeyValuePair<RedisKey, RedisValue>>
            {
                new KeyValuePair<RedisKey, RedisValue>(key, eventEntity.ToJson()),
                new KeyValuePair<RedisKey, RedisValue>("latest_timestamp", timestamp)
            };

            var isCommitted = await redisDatabase.StringSetAsync(keyValues.ToArray());

            if (isCommitted)
            {
                Console.WriteLine($"Committed successfully. Key: {key}, latest Timestamp: {timestamp}");
            }
            else
            {
                Console.WriteLine("Commit failed.");
            }
        }

        private static string ConvertToTimestamp(BsonDocument eventEntity)
        {
            var timestampBson = eventEntity["Timestamp"].AsBsonDateTime;
            return timestampBson.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ");
        }

        private static string? GetLatestTimestamp(IDatabase database)
        {
            var latestTimestamp = database.StringGet("latest_timestamp");
            if (string.IsNullOrEmpty(latestTimestamp)) return null;
            return latestTimestamp;
        }
    }
}

