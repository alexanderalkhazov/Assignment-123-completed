

public interface IEventEntityFactory
{
    EventEntity CreateEventEntity();
}

public class EventEntityFactory : IEventEntityFactory
{
    private readonly ProducerIniConfigManager _producerIniConfigManager;
    private int _currentReporterId;

    public EventEntityFactory(ProducerIniConfigManager producerIniConfigManager)
    {
        _producerIniConfigManager = producerIniConfigManager;
        _currentReporterId = producerIniConfigManager.ReporterIdCounterStart;
    }

    public EventEntity CreateEventEntity()
    {
        _currentReporterId += _producerIniConfigManager.IncrementValue;
        
        return new EventEntity
        {
            ReporterId = _currentReporterId,
            Timestamp = DateTime.UtcNow,
            MetricId = new Random().Next(_producerIniConfigManager.MetricIdMin, _producerIniConfigManager.MetricIdMax),
            MetricValue = new Random().Next(_producerIniConfigManager.MetricValueMin, _producerIniConfigManager.MetricValueMax),
            Message = _producerIniConfigManager.Message
        };
    }
}
