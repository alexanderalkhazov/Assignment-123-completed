using Confluent.Kafka;
using Newtonsoft.Json;
using IniParser;
using IniParser.Model;

internal class Program
{
    private static async Task Main(string[] args)
    {
        string currentDirectory = Directory.GetCurrentDirectory();
        string configFile = Path.Combine(currentDirectory, "..", "config.ini");
        var parser = new FileIniDataParser();
        IniData fileContext = parser.ReadFile(configFile);

        string kafkaServer = fileContext["KAFKA"]["KAFKA_SERVER"];
        string topicName = fileContext["KAFKA"]["KAFKA_TOPIC_NAME"];

        string message = fileContext["EVENT_DETAILS"]["MESSAGE"];
        int reporterId = int.Parse(fileContext["EVENT_DETAILS"]["REPORTER_ID_COUNTER_START"]);
        int countValue = int.Parse(fileContext["EVENT_DETAILS"]["INCREMENT_VALUE"]);
        int metricIdMin = int.Parse(fileContext["EVENT_DETAILS"]["METRIC_ID_MIN"]);
        int metricIdMax = int.Parse(fileContext["EVENT_DETAILS"]["METRIC_ID_MAX"]);
        int metricValueMin = int.Parse(fileContext["EVENT_DETAILS"]["METRIC_VALUE_MIN"]);
        int metricValueMax = int.Parse(fileContext["EVENT_DETAILS"]["METRIC_VALUE_MAX"]);
        int productionInterval = int.Parse(fileContext["EVENT_DETAILS"]["PRODUCTION_INTERVAL"]);

        if (
            string.IsNullOrEmpty(kafkaServer) ||
            string.IsNullOrEmpty(message) ||
            string.IsNullOrEmpty(topicName)
        )
        {
            Console.WriteLine("One or more environment variables are missing.");
            return;
        }

        var config = new ProducerConfig { BootstrapServers = kafkaServer };

        using (var producer = new ProducerBuilder<Null, string>(config).Build())
        {
            Console.WriteLine("Producer is running...");
            try
            {
                while (true)
                {
                    var eventEntity = new EventEntity
                    {
                        ReporterId = reporterId,
                        Timestamp = DateTime.UtcNow,
                        MetricId = new Random().Next(metricIdMin, metricIdMax),
                        MetricValue = new Random().Next(metricValueMin, metricValueMax),
                        Message = message
                    };
                    reporterId += countValue;

                    string json = JsonConvert.SerializeObject(eventEntity);
                    await producer.ProduceAsync(topicName, new Message<Null, string> { Value = json });
                    Console.WriteLine($"--Produced message: {json} to topic {topicName}--");
                    await Task.Delay(TimeSpan.FromSeconds(productionInterval));
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
            }
            finally
            {
                producer.Dispose();
            }
        }
    }
}