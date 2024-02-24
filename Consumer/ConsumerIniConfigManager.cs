using IniParser;
using IniParser.Model;

public class ConsumerIniConfigManager
{
    public string? KafkaServer { get; private set; }
    public string? TopicName { get; private set; }
    public string? GroupId { get; private set; }
    public string? MongoConnectionString { get; private set; }
    public string? MongoDatabaseName { get; private set; }
    public string? CollectionName { get; private set; }
    public int ConsumptionInterval { get; private set; }
    
    public ConsumerIniConfigManager(string configFilePath)
    {
        LoadConfigFromFile(configFilePath);
    }

    private void LoadConfigFromFile(string configFilePath)
    {
        var parser = new FileIniDataParser();
        IniData fileContext = parser.ReadFile(configFilePath);
        KafkaServer = fileContext["KAFKA"]["KAFKA_SERVER"];
        TopicName = fileContext["KAFKA"]["KAFKA_TOPIC_NAME"];
        GroupId = fileContext["KAFKA"]["GROUP_ID"];
        MongoConnectionString = fileContext["MONGO_DB"]["MONGO_CONNECTION_STRING"];
        MongoDatabaseName = fileContext["MONGO_DB"]["DATABASE_NAME"];
        CollectionName = fileContext["MONGO_DB"]["COLLECTION_NAME"];
        ConsumptionInterval = int.Parse(fileContext["EVENT_DETAILS"]["CONSUMPTION_INTERVAL"]);
    }
}
