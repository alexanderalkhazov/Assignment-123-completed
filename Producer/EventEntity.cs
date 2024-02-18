public class EventEntity
{
    public int ReporterId { get; set; }
    public DateTime Timestamp { get; set; }
    public int MetricId { get; set; }
    public int MetricValue { get; set; }
    public string? Message { get; set; }
}