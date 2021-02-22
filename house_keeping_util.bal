import ballerina/websubhub;
import ballerina/log;
import ballerina/lang.value;
import ballerinax/kafka;

const string REGISTERED_TOPICS = "registered-topics";
const string REGISTERED_CONSUMERS = "registered-consumers";

kafka:ProducerConfiguration houseKeepingProducerConfig = {
    bootstrapServers: "localhost:9092",
    clientId: "housekeeping-service",
    acks: "1",
    retryCount: 3
};
kafka:Producer houseKeepingService = checkpanic new (houseKeepingProducerConfig);

kafka:ConsumerConfiguration topicDetailsConsumerConfig = {
    bootstrapServers: "localhost:9092",
    groupId: "registered-topics-group",
    offsetReset: "earliest",
    topics: [ "registered-topics" ]
};
kafka:Consumer topicDetailsConsumer = checkpanic new (topicDetailsConsumerConfig);

kafka:ConsumerConfiguration subscriberDetailsConsumerConfig = {
    bootstrapServers: "localhost:9092",
    groupId: "registered-consumers-group",
    offsetReset: "earliest",
    topics: [ "registered-consumers" ]
};
kafka:Consumer subscriberDetailsConsumer = checkpanic new (subscriberDetailsConsumerConfig);

function persistTopicRegistrations(websubhub:TopicRegistration message) returns error? {
    websubhub:TopicRegistration[] topics = check getAvailableTopics();
    topics.push(message);
    json[] jsonData = topics;
    check publishHousekeepingData(REGISTERED_TOPICS, jsonData);
}

function persistTopicDeregistration(websubhub:TopicDeregistration message) returns error? {
    websubhub:TopicRegistration[] availableTopics = check getAvailableTopics();
    
    availableTopics = 
        from var registration in availableTopics
        where registration.topic != message.topic
        select registration;
    
    json[] jsonData = availableTopics;

    check publishHousekeepingData(REGISTERED_TOPICS, jsonData);
}

function persistSubscription(websubhub:VerifiedSubscription message) returns error? {
    Subscriber[] subscribers = check getAvailableSubscribers();
    Subscriber subscriber = getSubscriber(message);
    subscribers.push(subscriber);
    json[] jsonData = subscribers;
    check publishHousekeepingData(REGISTERED_CONSUMERS, jsonData);
}

function persistUnsubscription(websubhub:VerifiedUnsubscription message) returns error? {
    Subscriber[] subscribers = check getAvailableSubscribers();

    subscribers = 
        from var subscriber in subscribers
        where subscriber.hubTopic != message.hubTopic && subscriber.hubCallback != message.hubCallback
        select subscriber;
    
    json[] jsonData = subscribers;

    check publishHousekeepingData(REGISTERED_CONSUMERS, jsonData);
}

function publishHousekeepingData(string topicName, json payload) returns error? {
    log:print("Publishing content ", topic = topicName, payload = payload);

    byte[] serializedContent = payload.toJsonString().toBytes();

    check houseKeepingService->sendProducerRecord({ topic: topicName, value: serializedContent });

    check houseKeepingService->flushRecords();
}

function getAvailableTopics() returns websubhub:TopicRegistration[]|error {
    kafka:ConsumerRecord[] records = check topicDetailsConsumer->poll(1000);
    
    websubhub:TopicRegistration[] currentTopics = [];
    
    if (records.length() > 0) {
        kafka:ConsumerRecord lastRecord = records.pop();
        string|error lastPersistedData = string:fromBytes(lastRecord.value);
        
        if (lastPersistedData is string) {
            log:print("Last persisted-data set : ", message = lastPersistedData);

            json[] payload =  <json[]> check value:fromJsonString(lastPersistedData);

            foreach var data in payload {
                websubhub:TopicRegistration topic = check data.cloneWithType(websubhub:TopicRegistration);
                currentTopics.push(topic);
            }
        } else {
            log:printError("Error occurred while retrieving topic-details ", err = lastPersistedData);
        }
    }

    return currentTopics;
}

function getAvailableSubscribers() returns Subscriber[]|error {
    kafka:ConsumerRecord[] records = check subscriberDetailsConsumer->poll(1000);

    Subscriber[] currentSubscribers = [];

    if (records.length() > 0) {
        kafka:ConsumerRecord lastRecord = records.pop();
        string|error lastPersistedData = string:fromBytes(lastRecord.value);

        if (lastPersistedData is string) {
            log:print("Last persisted-data set : ", message = lastPersistedData);

            json[] payload =  <json[]> check value:fromJsonString(lastPersistedData);
            
            foreach var data in payload {
                Subscriber subscriber = check data.cloneWithType(Subscriber);
                currentSubscribers.push(subscriber);
            }
        } else {
            log:printError("Error occurred while retrieving subscriber-data ", err = lastPersistedData);
        }
    }

    return currentSubscribers;  
}