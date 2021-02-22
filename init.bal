import ballerina/log;
import ballerina/websubhub;

websubhub:TopicRegistration[] hubTopics = [];
Subscriber[] hubSubscribers = [];

function init() returns error? {
    log:print("Starting hub-service initialization");
    
    check initializeHubTopics();

    check initializeSubscriptions();

    check replayTopicRegistrations();

    check replaySubscriptions();

    check hubListener.attach(<websubhub:Service>hubService, "hub");

    check hubListener.'start();
}

function replayTopicRegistrations() returns error? {
    foreach var topic in hubTopics {
        registerTopic(topic, false);
    }
}

function replaySubscriptions() returns error? {
    foreach var subscriber in hubSubscribers {
        websubhub:VerifiedSubscription subscription = getSubscription(subscriber);
        check subscribe(subscription, false);
    }
}