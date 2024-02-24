using IniParser;
using IniParser.Model;

public class ProducerIniConfigManager
{
    public string? KafkaServer { get; private set; }
    public string? TopicName { get; private set; }
    public string? Message { get; private set; }
    public int ReporterIdCounterStart { get; private set; }
    public int IncrementValue { get; private set; }
    public int MetricIdMin { get; private set; }
    public int MetricIdMax { get; private set; }
    public int MetricValueMin { get; private set; }
    public int MetricValueMax { get; private set; }
    public int ProductionInterval { get; private set; }

    public ProducerIniConfigManager(string configFilePath)
    {
        LoadConfigFromFile(configFilePath);
    }

    private void LoadConfigFromFile(string configFilePath)
    {
        var parser = new FileIniDataParser();
        IniData fileContext = parser.ReadFile(configFilePath);
        KafkaServer = fileContext["KAFKA"]["KAFKA_SERVER"];
        TopicName = fileContext["KAFKA"]["KAFKA_TOPIC_NAME"];
        Message = fileContext["EVENT_DETAILS"]["MESSAGE"];
        ReporterIdCounterStart = int.Parse(fileContext["EVENT_DETAILS"]["REPORTER_ID_COUNTER_START"]);
        IncrementValue = int.Parse(fileContext["EVENT_DETAILS"]["INCREMENT_VALUE"]);
        MetricIdMin = int.Parse(fileContext["EVENT_DETAILS"]["METRIC_ID_MIN"]);
        MetricIdMax = int.Parse(fileContext["EVENT_DETAILS"]["METRIC_ID_MAX"]);
        MetricValueMin = int.Parse(fileContext["EVENT_DETAILS"]["METRIC_VALUE_MIN"]);
        MetricValueMax = int.Parse(fileContext["EVENT_DETAILS"]["METRIC_VALUE_MAX"]);
        ProductionInterval = int.Parse(fileContext["EVENT_DETAILS"]["PRODUCTION_INTERVAL"]);
    }
}
