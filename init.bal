import ballerina/log;
import ballerina/websubhub;

listener websubhub:Listener hubListener = new websubhub:Listener(9090);

public function main() returns error? {
    log:printInfo("Starting hub-service initialization");
    
    // Initialize the Hub
    check replayTopicRegistrations();
    check replaySubscriptions();

    // Start the Hub
    check hubListener.attach(<websubhub:Service>hubService, "hub");
    check hubListener.'start();
}

function replayTopicRegistrations() returns error? {
    websubhub:TopicRegistration[] availableTopics = check getAvailableTopics();
    foreach var topic in availableTopics {
        registerTopic(topic, false);
    }
}

function replaySubscriptions() returns error? {
    websubhub:VerifiedSubscription[] availableSubscribers = check getAvailableSubscribers();
    foreach var subscription in availableSubscribers {
        check subscribe(subscription, false);
    }
}
