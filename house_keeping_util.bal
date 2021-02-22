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
    log:print("persisting topic-registration ", topicDetails = message);

    hubTopics.push(message);

    json[] jsonData = hubTopics;

    check publishHousekeepingData(REGISTERED_TOPICS, jsonData);
}

function persistTopicDeregistration(websubhub:TopicDeregistration message) returns error? {
    hubTopics = 
        from var registration in hubTopics
        where registration.topic != message.topic
        select registration;
    
    json[] jsonData = hubTopics;

    check publishHousekeepingData(REGISTERED_TOPICS, jsonData);
}

function persistSubscription(websubhub:VerifiedSubscription message) returns error? {
    Subscriber subscriber = getSubscriber(message);
    
    hubSubscribers.push(subscriber);

    json[] jsonData = hubSubscribers;

    check publishHousekeepingData(REGISTERED_CONSUMERS, jsonData);
}

function persistUnsubscription(websubhub:VerifiedUnsubscription message) returns error? {
    hubSubscribers = 
        from var subscriber in hubSubscribers
        where subscriber.hubTopic != message.hubTopic && subscriber.hubCallback != message.hubCallback
        select subscriber;
    
    json[] jsonData = hubSubscribers;

    check publishHousekeepingData(REGISTERED_CONSUMERS, jsonData);
}

function publishHousekeepingData(string topicName, json payload) returns error? {
    log:print("Publishing content ", topic = topicName, payload = payload);

    byte[] serializedContent = payload.toJsonString().toBytes();

    check houseKeepingService->sendProducerRecord({ topic: topicName, value: serializedContent });

    check houseKeepingService->flushRecords();
}

function initializeHubTopics() returns error? {
    kafka:ConsumerRecord[] records = check topicDetailsConsumer->poll(1000);

    foreach var kafkaRecord in records {
        byte[] content = kafkaRecord.value;
        string|error retrievedData = string:fromBytes(content);
            
        if (retrievedData is string) {
            log:print("Received topic-details : ", message = retrievedData);

            json[] payload =  <json[]> check value:fromJsonString(retrievedData);

            websubhub:TopicRegistration[] currentTopics = [];
            foreach var data in payload {
                websubhub:TopicRegistration topic = check data.cloneWithType(websubhub:TopicRegistration);
                currentTopics.push(topic);
            }

            hubTopics = currentTopics;

        } else {
            log:printError("Error occurred while retrieving topic-details ", err = retrievedData);
        }
    }
}

function initializeSubscriptions() returns error? {
    kafka:ConsumerRecord[] records = check subscriberDetailsConsumer->poll(1000);

    foreach var kafkaRecord in records {
        byte[] content = kafkaRecord.value;
        string|error retrievedData = string:fromBytes(content);
            
        if (retrievedData is string) {
            log:print("Received subscriber-details : ", message = retrievedData);

            json[] payload =  <json[]> check value:fromJsonString(retrievedData);

            Subscriber[] currentSubscribers = [];
            foreach var data in payload {
                Subscriber subscriber = check data.cloneWithType(Subscriber);
                currentSubscribers.push(subscriber);
            }
            

            hubSubscribers = currentSubscribers;

        } else {
            log:printError("Error occurred while retrieving subscriber-data ", err = retrievedData);
        }
    }    
}