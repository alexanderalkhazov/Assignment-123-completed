using IniParser;
using IniParser.Model;

public class ETLIniConfigManager
{
    public string? MongoConnectionString { get; private set; }
    public string? CollectionName { get; private set; }
    public string? MongoDatabaseName { get; private set; }
    public string? RedisHostName { get; private set; }
    public string? RedisPort { get; private set; }
    public int EtlSleepDuration { get; private set; }
    public string? RedisServerString { get; private set; }

    public ETLIniConfigManager(string configFilePath)
    {
        LoadConfigFromFile(configFilePath);
    }

    private void LoadConfigFromFile(string configFilePath)
    {
        var parser = new FileIniDataParser();
        IniData fileContext = parser.ReadFile(configFilePath);
        MongoConnectionString = fileContext["MONGO_DB"]["MONGO_CONNECTION_STRING"];
        CollectionName = fileContext["MONGO_DB"]["COLLECTION_NAME"];
        MongoDatabaseName = fileContext["MONGO_DB"]["DATABASE_NAME"];
        RedisHostName = fileContext["REDIS"]["REDIS_HOST_NAME"];
        RedisPort = fileContext["REDIS"]["REDIS_PORT"];
        EtlSleepDuration = int.Parse(fileContext["REDIS"]["ETL_SLEEP_DURATION"]);
        RedisServerString = $"{RedisHostName}:{RedisPort}";
    }
}
