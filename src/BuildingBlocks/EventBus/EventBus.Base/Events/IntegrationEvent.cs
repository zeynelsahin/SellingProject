using Newtonsoft.Json;

namespace EventBus.Base.Events;

public class IntegrationEvent
{
    public IntegrationEvent()
    {
        Id = Guid.NewGuid();
        CreateDate = DateTime.Now;
    }

    [JsonConstructor]
    public IntegrationEvent(Guid id, DateTime createDate)
    {
        Id = id;
        CreateDate = createDate;
    }

    [JsonProperty] public Guid Id { get; }
    [JsonProperty] public DateTime CreateDate { get; }
}