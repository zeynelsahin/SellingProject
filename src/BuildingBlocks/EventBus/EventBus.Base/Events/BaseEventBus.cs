using EventBus.Base.Abstraction;
using EventBus.Base.SubManagers;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json;

namespace EventBus.Base.Events;

public abstract class BaseEventBus : IEventBus
{
    public readonly IServiceProvider ServiceProvider;
    public readonly IEventBusSubscriptionManager SubsManager;
    public EventBusConfig EventConfig { get; set; }

    public BaseEventBus(IServiceProvider serviceProvider, EventBusConfig eventBusConfig)
    {
        ServiceProvider = serviceProvider;
        SubsManager = new InMemoryEventBusSubscriptionManager(ProcessEventName);
        EventConfig = eventBusConfig;
    }

    public virtual string ProcessEventName(string eventName)
    {
        if (EventConfig.DeleteEvenPrefix)
        {
            eventName = eventName.TrimStart(EventConfig.EventNamePrefix.ToArray());
        }

        if (EventConfig.DeleteEventSuffix)
        {
            eventName = eventName.TrimEnd(EventConfig.EventNameSuffix.ToArray());
        }

        return eventName;
    }

    public virtual string GetSubName(string eventName)
    {
        return $"{EventConfig.SubscriberClientAppName}.{ProcessEventName(eventName)}";
    }

    public virtual void Dispose()
    {
        EventConfig = null;
    }

    public async Task<bool> ProcessEvent(string eventName, string message)
    {
        eventName = ProcessEventName(eventName);
        var processed = false;
        if (SubsManager.HasSubscriptionsForEvent(eventName))
        {
            var subscriptions = SubsManager.GetHandlersForEvent(eventName);

            using (var scope = ServiceProvider.CreateScope())
            {
                foreach (var subscription in subscriptions)
                {
                    var handler = ServiceProvider.GetService(subscription.HandlerType);
                    if (handler == null) continue;
                    var eventType = SubsManager.GetEventTypeByName($"{EventConfig.EventNamePrefix}{eventName}{EventConfig.DeleteEventSuffix}");
                    var integrationEvent = JsonConvert.DeserializeObject(message, eventType);

                    var concreteType = typeof(IIntegrationEventHandler<>).MakeGenericType(eventType);
                    await (Task)concreteType.GetMethod("Handle")!.Invoke(handler, new object?[] { integrationEvent })!;
                }
            }

            processed = true;
        }

        return true;
    }

    public abstract void Publish(IntegrationEvent @event);

    public abstract void Subscribe<T, TH>() where T : IntegrationEvent where TH : IIntegrationEventHandler<T>;

    public abstract void UnSubscribe<T, TH>() where T : IntegrationEvent where TH : IIntegrationEventHandler<T>;
}