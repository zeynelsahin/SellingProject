using System.Text;
using EventBus.Base;
using EventBus.Base.Events;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Management;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace EventBus.AzureServiceBus;

public class EventBusServiceBus : BaseEventBus
{
    public EventBusServiceBus(IServiceProvider serviceProvider, EventBusConfig eventConfig) : base(serviceProvider, eventConfig)
    {
        _logger = serviceProvider.GetService(typeof(ILogger<EventBusServiceBus>)) as ILogger<EventBusServiceBus>;
        _managementClient = new ManagementClient(eventConfig.EventBusConnectionString);
        _topicClient = CreateTopicClient();
    }

    private ITopicClient _topicClient;
    private ManagementClient _managementClient;
    private readonly ILogger? _logger;

    private ITopicClient CreateTopicClient()
    {
        if (_topicClient == null || _topicClient.IsClosedOrClosing) _topicClient = new TopicClient(EventConfig.EventBusConnectionString, EventConfig.DefaultTopicName, RetryPolicy.Default);

        if (!_managementClient.TopicExistsAsync(EventConfig.DefaultTopicName).GetAwaiter().GetResult()) _managementClient.CreateTopicAsync(EventConfig.DefaultTopicName).GetAwaiter().GetResult();

        return _topicClient;
    }

    public override void Publish(IntegrationEvent @event)
    {
        var eventName = @event.GetType().Name;
        eventName = ProcessEventName(eventName);
        var eventString = JsonConvert.SerializeObject(@event);
        var bodyArray = Encoding.UTF8.GetBytes(eventString);
        var message = new Message()
        {
            MessageId = Guid.NewGuid().ToString(),
            Body = bodyArray,
            Label = eventName
        };


        _topicClient.SendAsync(message).GetAwaiter().GetResult();
    }

    public override void Subscribe<T, TH>()
    {
        var eventName = typeof(T).Name;
        eventName = ProcessEventName(eventName);
        if (!SubsManager.HasSubscriptionsForEvent(eventName))
        {
            var subscriptionClient = CreateSubscriptionClientIfNotExists(eventName);
            RegisterSubscriptionClientMessageHander(subscriptionClient);
        }
        _logger.LogInformation("Subscribing to event {EventName} with {EventHandler}",eventName,typeof(TH).Name);
        SubsManager.AddSubscription<T,TH>();
    }

    public override void UnSubscribe<T, TH>()
    {
        var eventName = typeof(T).Name;
        try
        {
            var subscriptionClient = CreateSubscriptionClient(eventName);
            subscriptionClient.RemoveRuleAsync(eventName).GetAwaiter().GetResult();
        }
        catch (MessagingEntityNotFoundException )
        {
            _logger.LogWarning("The messaging entity {eventName} Could not be found",eventName);
            _logger.LogInformation("Unsubscribing from event {EventName}",eventName);
            SubsManager.RemoveSubscription<T,TH>();
        }
    }

    private void RegisterSubscriptionClientMessageHander(ISubscriptionClient subscriptionClient)
    {
        subscriptionClient.RegisterMessageHandler(async (message, token) =>
        {
            var eventName = $"{message.Label}";
            var messageDate = Encoding.UTF8.GetString(message.Body);

            if (await ProcessEvent(ProcessEventName(eventName),messageDate))
            {
                await subscriptionClient.CompleteAsync(message.SystemProperties.LockToken);
            }
        },new MessageHandlerOptions(ExceptionReceivedHandler){MaxConcurrentCalls = 10,AutoComplete = false});
    }

    private Task ExceptionReceivedHandler(ExceptionReceivedEventArgs arg)
    {
        var ex = arg.Exception;
        var context = arg.ExceptionReceivedContext;
        _logger.LogError(ex,"Error handling message: {ExceptionMessage} - Context : {@ExceptionContext}",ex.Message,context);
        return Task.CompletedTask;
    }

    private ISubscriptionClient CreateSubscriptionClientIfNotExists(string eventName)
    {
        var subClient = CreateSubscriptionClient(eventName);
        var exists = _managementClient.SubscriptionExistsAsync(EventConfig.DefaultTopicName, GetSubName(eventName)).GetAwaiter().GetResult();
        if (!exists)
        {
            _managementClient.CreateSubscriptionAsync(EventConfig.DefaultTopicName, GetSubName(eventName)).GetAwaiter().GetResult();
            RemoveDefaultRule(subClient);
        }

        CreateRuleIfNotExists(ProcessEventName(eventName), subClient);
        return subClient;
    }

    private void CreateRuleIfNotExists(string eventName, ISubscriptionClient subscriptionClient)
    {
        bool ruleExists;
        try
        {
            var rule = _managementClient.GetRuleAsync(EventConfig.DefaultTopicName, eventName, eventName).GetAwaiter().GetResult();
            ruleExists = rule != null;
        }
        catch (MessagingEntityNotFoundException e)
        {
            ruleExists = false;
        }

        if (!ruleExists)
        {
            subscriptionClient.AddRuleAsync(new RuleDescription()
            {
                Filter = new CorrelationFilter(){Label = eventName}
                ,Name = eventName
            }).GetAwaiter().GetResult();
        }
    }
    private void RemoveDefaultRule(SubscriptionClient subscriptionClient)
    {
        try
        {
            subscriptionClient.RemoveRuleAsync(RuleDescription.DefaultRuleName).GetAwaiter().GetResult();
        }
        catch (MessagingEntityNotFoundException)
        {
         _logger!.LogWarning("The Messaging entity{DefaultName} Could not be found",RuleDescription.DefaultRuleName);   
        }
    }

    private SubscriptionClient CreateSubscriptionClient(string eventName)
    {
        return new SubscriptionClient(EventConfig.DefaultTopicName, EventConfig.DefaultTopicName, GetSubName(eventName));
    }

    public override void Dispose()
    {
        base.Dispose();
        _managementClient.CloseAsync().GetAwaiter().GetResult();
        _topicClient.CloseAsync().GetAwaiter().GetResult();
        _topicClient = null;
        _managementClient = null;
    }
}