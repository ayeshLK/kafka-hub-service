import ballerina/log;
import ballerina/websubhub;
import ballerina/io;
import ballerinax/kafka;

listener websubhub:Listener hubListener = new (9090);

public function main() returns error? {
    log:printInfo("Starting Hub-Service");
    
    // Initialize the Hub
    _ = start updateTopicDetails();
    check replaySubscriptions();
    
    // Start the Hub
    check hubListener.attach(hubService, "hub");
    check hubListener.'start();
}

isolated function updateTopicDetails() returns error? {
    while true {
        websubhub:TopicRegistration[]|error? topicDetails = getAvailableTopics();
        io:println("Executing topic-update with available topic details ", topicDetails is websubhub:TopicRegistration[]);
        if (topicDetails is websubhub:TopicRegistration[]) {
            lock {
                registeredTopics.removeAll();
            }
            foreach var topic in topicDetails.cloneReadOnly() {
                string topicName = generateTopicName(topic.topic);
                lock {
                    registeredTopics[topicName] = topic.cloneReadOnly();
                }
            }
        }
    }
    _ = check topicDetailsConsumer->close(5);
}

function replaySubscriptions() returns error? {
    websubhub:VerifiedSubscription[] availableSubscribers = check getAvailableSubscribers();
    foreach var subscription in availableSubscribers {
        string groupName = generateGroupName(subscription.hubTopic, subscription.hubCallback);
        kafka:Consumer consumerEp = check createMessageConsumer(subscription);
        websubhub:HubClient hubClientEp = check new (subscription);
        boolean shouldRunNotification = true;
        lock {
            subscribers[groupName] = shouldRunNotification;
        }
        var result = start notifySubscriber(hubClientEp, consumerEp, groupName);
    }
}
