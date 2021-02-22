import ballerinax/kafka;
import ballerina/websubhub;
import ballerina/crypto;
import ballerina/log;
import ballerina/mime;
import ballerina/lang.value;

map<string> registeredTopics = {};
map<future<error?>> registeredConsumers = {};

kafka:ProducerConfiguration mainProducerConfig = {
    bootstrapServers: "localhost:9092",
    clientId: "main-producer",
    acks: "1",
    retryCount: 3
};

kafka:Producer mainProducer = checkpanic new (mainProducerConfig);

function registerTopic(websubhub:TopicRegistration message, boolean persist = true) {
    string topicId = crypto:hashSha1(message.topic.toBytes()).toBase64();
    registeredTopics[topicId] = message.topic;
    
    if (persist) {
        var persistingResult = persistTopicRegistrations(message);
        if (persistingResult is error) {
            log:printError("Error occurred while persisting the topic-registration ", err = persistingResult);
        }
    }
}

function deregisterTopic(websubhub:TopicDeregistration message) {
    string topicId = crypto:hashSha1(message.topic.toBytes()).toBase64();
    if (registeredTopics.hasKey(topicId)) {
        string deletedTopic = registeredTopics.remove(topicId);
    }

    var persistingResult = persistTopicDeregistration(message);
    if (persistingResult is error) {
        log:printError("Error occurred while persisting the topic-deregistration ", err = persistingResult);
    }
}

function publishContent(websubhub:UpdateMessage message) returns error? {
    string topicId = crypto:hashSha1(message.hubTopic.toBytes()).toBase64();
    if (registeredTopics.hasKey(topicId)) {
        string topicName = generateTopicName(message.hubTopic);

        log:print("Distributing content to ", Topic = topicName);

        // here we have assumed that the payload will be in `json` format
        json payload = <json>message.content;

        byte[] content = payload.toJsonString().toBytes();

        check mainProducer->sendProducerRecord({ topic: topicName, value: content });

        check mainProducer->flushRecords();
    } else {
        return error websubhub:UpdateMessageError("Topic [" + message.hubTopic + "] is not registered with the Hub");
    }
}

function validateSubscription(websubhub:Subscription message) returns websubhub:SubscriptionDeniedError? {
    string topicId = crypto:hashSha1(message.hubTopic.toBytes()).toBase64();
    if (!registeredTopics.hasKey(topicId)) {
        return error websubhub:SubscriptionDeniedError("Topic [" + message.hubTopic + "] is not registered with the Hub");
    }
}

function subscribe(websubhub:VerifiedSubscription message, boolean persist = true) returns error? {
    string topicName = generateTopicName(message.hubTopic);
    string groupId = generateGroupId(message.hubTopic, message.hubCallback);
    kafka:Consumer consumerEp = check getConsumer([ topicName ], groupId, false);
    websubhub:HubClient hubClientEp = check new (message);
    var result = start notifySubscriber(hubClientEp, consumerEp);
    registeredConsumers[groupId] = result;

    if (persist) {
        var persistingResult = persistSubscription(message);
        if (persistingResult is error) {
            log:printError("Error occurred while persisting the subscription ", err = persistingResult);
        }    
    }
}

function notifySubscriber(websubhub:HubClient clientEp, kafka:Consumer consumerEp) returns error? {
    while (true) {
        kafka:ConsumerRecord[] records = check consumerEp->poll(1000);

        foreach var kafkaRecord in records {
            byte[] content = kafkaRecord.value;
            string|error message = string:fromBytes(content);
            
            if (message is string) {
                log:print("Received message : ", message = message);

                json payload =  check value:fromJsonString(message);
                websubhub:ContentDistributionMessage distributionMsg = {
                    content: payload,
                    contentType: mime:APPLICATION_JSON
                };

                var publishResponse = clientEp->notifyContentDistribution(distributionMsg);

                if (publishResponse is error) {
                    log:printError("Error occurred while sending notification to subscriber ", err = publishResponse);
                } else {
                    _ = check consumerEp->commit();
                }

            } else {
                log:printError("Error occurred while retrieving message data", err = message);
            }
        }
    }
}

function validateUnsubscription(websubhub:Unsubscription message) returns websubhub:UnsubscriptionDeniedError? {
    string topicId = crypto:hashSha1(message.hubTopic.toBytes()).toBase64();
    if (!registeredTopics.hasKey(topicId)) {
        return error websubhub:UnsubscriptionDeniedError("Topic [" + message.hubTopic + "] is not registered with the Hub");
    } else {
        string groupId = generateGroupId(message.hubTopic, message.hubCallback);
        if (!registeredConsumers.hasKey(groupId)) {
            return error websubhub:UnsubscriptionDeniedError("Could not find a valid subscriber for Topic [" 
                                + message.hubTopic + "] and Callback [" + message.hubCallback + "]");
        }
    }    
}

function unsubscribe(websubhub:VerifiedUnsubscription message) returns error? {
    string groupId = generateGroupId(message.hubTopic, message.hubCallback);
    var registeredConsumer = registeredConsumers[groupId];
    if (registeredConsumer is future<error?>) {
         _ = registeredConsumer.cancel();
        var result = registeredConsumers.remove(groupId);
    }  

    var persistingResult = persistUnsubscription(message);
    if (persistingResult is error) {
        log:printError("Error occurred while persisting the unsubscription ", err = persistingResult);
    }  
}