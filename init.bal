import ballerina/log;
import ballerina/websubhub;

function init() returns error? {
    log:print("Starting hub-service initialization");
    
    websubhub:TopicRegistration[] availableTopics = check getAvailableTopics();

    Subscriber[] availableSubscribers = check getAvailableSubscribers();

    check replayTopicRegistrations(availableTopics);

    check replaySubscriptions(availableSubscribers);

    check hubListener.attach(<websubhub:Service>hubService, "hub");

    check hubListener.'start();
}

function replayTopicRegistrations(websubhub:TopicRegistration[] topics) returns error? {
    foreach var topic in topics {
        registerTopic(topic, false);
    }
}

function replaySubscriptions(Subscriber[] subscribers) returns error? {
    foreach var subscriber in subscribers {
        websubhub:VerifiedSubscription subscription = getSubscription(subscriber);
        check subscribe(subscription, false);
    }
}