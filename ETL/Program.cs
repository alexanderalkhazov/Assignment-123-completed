using MongoDB.Driver;
using StackExchange.Redis;
using MongoDB.Bson;
using IniParser;
using IniParser.Model;
using System.Globalization;

namespace ETL
{
    class Program
    {
        static async Task Main(string[] args)
        {
            string currentDirectory = Directory.GetCurrentDirectory();
            string configFile = Path.Combine(currentDirectory, "..", "config.ini");
            var parser = new FileIniDataParser();
            IniData fileContext = parser.ReadFile(configFile);

            string mongoConnectionString = fileContext["MONGO_DB"]["MONGO_CONNECTION_STRING"];
            string collectionName = fileContext["MONGO_DB"]["COLLECTION_NAME"];
            string mongoDatabaseName = fileContext["MONGO_DB"]["DATABASE_NAME"];

            string redisHostName = fileContext["REDIS"]["REDIS_HOST_NAME"];
            string redisPort = fileContext["REDIS"]["REDIS_PORT"];
            int etlSleepDuration = int.Parse(fileContext["REDIS"]["ETL_SLEEP_DURATION"]);

            string redisServer = $"{redisHostName}:{redisPort}";

            if (
                string.IsNullOrEmpty(mongoConnectionString) ||
                string.IsNullOrEmpty(redisServer) ||
                string.IsNullOrEmpty(mongoDatabaseName) ||
                string.IsNullOrEmpty(collectionName)
            )
            {
                Console.WriteLine("One or more environment variables are missing.");
                return;
            }

            Console.WriteLine("ETL proccess is running.");

            try
            {
                var mongoClient = new MongoClient(mongoConnectionString);
                var mongoDatabase = mongoClient.GetDatabase(mongoDatabaseName);
                var mongoCollection = mongoDatabase.GetCollection<BsonDocument>(collectionName);

                var redis = ConnectionMultiplexer.Connect(redisServer);
                var redisDatabase = redis.GetDatabase();

                while (true)
                {
                    Console.WriteLine("ETL processing...");

                    string? latestTimestamp = GetLatestTimestamp(redisDatabase);

                    if (!string.IsNullOrEmpty(latestTimestamp))
                    {
                        Console.WriteLine($"--latest timestamp {latestTimestamp}--");
                        DateTime latestDateTimeTimestamp = DateTime.Parse(latestTimestamp, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
                        var filter = Builders<BsonDocument>.Filter.Gt("Timestamp", latestDateTimeTimestamp);
                        var sort = Builders<BsonDocument>.Sort.Ascending("Timestamp");
                        var newEventObjects = await mongoCollection.Find(filter).Sort(sort).ToListAsync();
                        foreach (var eventObj in newEventObjects)
                        {
                            Console.WriteLine($"new eventObj: {eventObj["Timestamp"]}");
                            BsonDateTime timestampBson = eventObj["Timestamp"].AsBsonDateTime;
                            DateTime timestampUtc = timestampBson.ToUniversalTime();
                            UpdateLatestTimestamp(timestampUtc.ToString("yyyy-MM-ddTHH:mm:ss.fffZ"), redisDatabase);
                            SaveNewObjectToRedis(redisDatabase, eventObj);
                        }
                    }
                    else
                    {
                        Console.WriteLine("No last timestamp.");
                        var filter = Builders<BsonDocument>.Filter.Empty;
                        var newObjects = await mongoCollection.Find(filter).ToListAsync();
                        foreach (var eventObj in newObjects)
                        {
                            BsonDateTime timestampBson = eventObj["Timestamp"].AsBsonDateTime;
                            DateTime timestampUtc = timestampBson.ToUniversalTime();
                            UpdateLatestTimestamp(timestampUtc.ToString("yyyy-MM-ddTHH:mm:ss.fffZ"), redisDatabase);
                            SaveNewObjectToRedis(redisDatabase, eventObj);
                        }
                    }
                    Console.WriteLine("Cool down...");
                    Thread.Sleep(etlSleepDuration);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        private static void SaveNewObjectToRedis(IDatabase redisDatabase, BsonDocument eventObj)
        {
            string key = $"{eventObj["ReporterId"].AsInt32}:{eventObj["Timestamp"].AsBsonDateTime}";
            redisDatabase.StringSet(key, eventObj.ToJson());
            Console.WriteLine($"--Inserted new object into Redis: {key}--");
        }

        public static string? GetLatestTimestamp(IDatabase database)
        {
            var latestTimestamp = database.StringGet("latest_timestamp");
            if (string.IsNullOrEmpty(latestTimestamp)) return null;
            return latestTimestamp;
        }

        public static void UpdateLatestTimestamp(string timestamp, IDatabase database)
        {
            database.StringSet("latest_timestamp", timestamp);
            Console.WriteLine($"---Updated new latest_timestamp: {timestamp}---");
        }
    }
}

