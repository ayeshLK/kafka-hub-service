import ballerinax/kafka;
import ballerina/websubhub;
import ballerina/log;
import ballerina/mime;
import ballerina/lang.value;

map<string> registeredTopics = {};
map<future<error?>> registeredConsumers = {};

kafka:ProducerConfiguration producerConfig = {
    clientId: "update-message",
    acks: "1",
    retryCount: 3
};

kafka:Producer updateMessageProducer = check new ("localhost:9092", producerConfig);

function registerTopic(websubhub:TopicRegistration message, boolean persist = true) {
    log:printInfo("Received topic-registration request ", request = message);
    string topicName = generateTopicName(message.topic);
    registeredTopics[topicName] = message.topic;
    
    if (persist) {
        error? persistingResult = persistTopicRegistrations(message);
        if (persistingResult is error) {
            log:printError("Error occurred while persisting the topic-registration ", err = persistingResult.message());
        }
    }
}

function deregisterTopic(websubhub:TopicRegistration message) {
    log:printInfo("Received topic-deregistration request ", request = message);
    string topicName = generateTopicName(message.topic);
    if (registeredTopics.hasKey(topicName)) {
        string deletedTopic = registeredTopics.remove(topicName);
    }

    error? persistingResult = persistTopicDeregistration(message);
    if (persistingResult is error) {
        log:printError("Error occurred while persisting the topic-deregistration ", err = persistingResult.message());
    }
}

function subscribe(websubhub:VerifiedSubscription message, boolean persist = true) returns error? {
    log:printInfo("Received subscription request ", request = message);
    string topicName = generateTopicName(message.hubTopic);
    string groupName = generateGroupName(message.hubTopic, message.hubCallback);
    kafka:Consumer consumerEp = check getConsumer([ topicName ], groupName, false);
    websubhub:HubClient hubClientEp = check new (message);
    var result = start notifySubscriber(hubClientEp, consumerEp);
    registeredConsumers[groupName] = result;

    if (persist) {
        error? persistingResult = persistSubscription(message);
        if (persistingResult is error) {
            log:printError("Error occurred while persisting the subscription ", err = persistingResult.message());
        }    
    }
}

function publishContent(websubhub:UpdateMessage message) returns error? {
    string topicName = generateTopicName(message.hubTopic);
    if (registeredTopics.hasKey(topicName)) {
        log:printInfo("Distributing content to ", Topic = topicName);

        // here we have assumed that the payload will be in `json` format
        json payload = <json>message.content;

        byte[] content = payload.toJsonString().toBytes();

        check updateMessageProducer->send({ topic: topicName, value: content });

        check updateMessageProducer->'flush();
    } else {
        return error websubhub:UpdateMessageError("Topic [" + message.hubTopic + "] is not registered with the Hub");
    }
}

function notifySubscriber(websubhub:HubClient clientEp, kafka:Consumer consumerEp) returns error? {
    while (true) {
        kafka:ConsumerRecord[] records = check consumerEp->poll(1000);

        foreach var kafkaRecord in records {
            byte[] content = kafkaRecord.value;
            string|error message = string:fromBytes(content);
            
            if (message is string) {
                log:printInfo("Received message : ", message = message);

                json payload =  check value:fromJsonString(message);
                websubhub:ContentDistributionMessage distributionMsg = {
                    content: payload,
                    contentType: mime:APPLICATION_JSON
                };

                var publishResponse = clientEp->notifyContentDistribution(distributionMsg);

                if (publishResponse is error) {
                    log:printError("Error occurred while sending notification to subscriber ", err = publishResponse.message());
                } else {
                    _ = check consumerEp->commit();
                }

            } else {
                log:printError("Error occurred while retrieving message data", err = message.message());
            }
        }
    }
}
