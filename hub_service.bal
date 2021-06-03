import ballerina/websubhub;
import ballerina/log;
import ballerina/http;
import ballerinax/kafka;

isolated map<string> registeredTopics = {};

isolated function isTopicAvailable(string topicName) returns boolean {
    lock {
        return registeredTopics.hasKey(topicName);
    }
}

isolated function addTopic(string topicName, string topic) {
    lock {
        registeredTopics[topicName] = topic;
    }
}

isolated function removeTopic(string topicName) returns string {
    lock {
        return registeredTopics.remove(topicName);
    }
}


isolated map<future<error?>> registeredConsumers = {};

isolated function isConsumerAvailable(string groupName) returns boolean {
    lock {
        return registeredConsumers.hasKey(groupName);
    }
}

// isolated function addConsumer(string groupName, future<error?> result) {
//     lock {
//         registeredConsumers[groupName] = result;
//     }
// }

isolated function removeConsumer(string groupName) {
    lock {
        var registeredConsumer = registeredConsumers[groupName];
        if (registeredConsumer is future<error?>) {
             _ = registeredConsumer.cancel();
            var result = registeredConsumers.remove(groupName);
        }
    }
}

websubhub:Service hubService = service object {
    isolated remote function onRegisterTopic(websubhub:TopicRegistration message, http:Headers headers)
                                returns websubhub:TopicRegistrationSuccess|websubhub:TopicRegistrationError|error {
        // check authorize(headers, ["register_topic"]);
        check self.registerTopic(message);
        return websubhub:TOPIC_REGISTRATION_SUCCESS;
    }

    isolated function registerTopic(websubhub:TopicRegistration message) returns websubhub:TopicRegistrationError? {
        log:printInfo("Received topic-registration request ", request = message);
        string topicName = generateTopicName(message.topic);
        if (isTopicAvailable(topicName)) {
            return error websubhub:TopicRegistrationError("Topic has already registered with the Hub");
        }
        addTopic(topicName, message.topic);
        error? persistingResult = persistTopicRegistrations(message);
        if (persistingResult is error) {
            log:printError("Error occurred while persisting the topic-registration ", err = persistingResult.message());
        }
    }

    isolated remote function onDeregisterTopic(websubhub:TopicDeregistration message, http:Headers headers)
                        returns websubhub:TopicDeregistrationSuccess|websubhub:TopicDeregistrationError|error {
        // check authorize(headers, ["deregister_topic"]);
        self.deregisterTopic(message);
        return websubhub:TOPIC_DEREGISTRATION_SUCCESS;
    }

    isolated function deregisterTopic(websubhub:TopicRegistration message) {
        log:printInfo("Received topic-deregistration request ", request = message);
        string topicName = generateTopicName(message.topic);
        if (isTopicAvailable(topicName)) {
            string deletedTopic = removeTopic(topicName);
        }
        error? persistingResult = persistTopicDeregistration(message);
        if (persistingResult is error) {
            log:printError("Error occurred while persisting the topic-deregistration ", err = persistingResult.message());
        }
    }

    isolated remote function onUpdateMessage(websubhub:UpdateMessage msg, http:Headers headers)
               returns websubhub:Acknowledgement|websubhub:UpdateMessageError|error {  
        // check authorize(headers, ["update_content"]);
        check self.updateMessage(msg);
        return websubhub:ACKNOWLEDGEMENT;
    }

    isolated function updateMessage(websubhub:UpdateMessage msg) returns websubhub:UpdateMessageError? {
        log:printInfo("Received content-update request ", request = msg.toString());
        string topicName = generateTopicName(msg.hubTopic);
        if (isTopicAvailable(topicName)) {
            error? errorResponse = publishContent(msg, topicName);
            if (errorResponse is websubhub:UpdateMessageError) {
                return errorResponse;
            } else if (errorResponse is error) {
                log:printError("Error occurred while publishing the content ", errorMessage = errorResponse.message());
                return error websubhub:UpdateMessageError(errorResponse.message());
            }
        } else {
            return error websubhub:UpdateMessageError("Topic [" + msg.hubTopic + "] is not registered with the Hub");
        }
    }
    
    isolated remote function onSubscription(websubhub:Subscription message, http:Headers headers)
                returns websubhub:SubscriptionAccepted|websubhub:BadSubscriptionError|error {
        // check authorize(headers, ["subscribe"]);
        return websubhub:SUBSCRIPTION_ACCEPTED;
    }

    isolated remote function onSubscriptionValidation(websubhub:Subscription message)
                returns websubhub:SubscriptionDeniedError? {
        log:printInfo("Received subscription-validation request ", request = message.toString());

        string topicName = generateTopicName(message.hubTopic);
        string groupName = generateGroupName(message.hubTopic, message.hubCallback);
        if (!isTopicAvailable(topicName)) {
            return error websubhub:SubscriptionDeniedError("Topic [" + message.hubTopic + "] is not registered with the Hub");
        } else if (isConsumerAvailable(groupName)) {
            return error websubhub:SubscriptionDeniedError("Subscriber has already registered with the Hub");
        }
    }

    remote function onSubscriptionIntentVerified(websubhub:VerifiedSubscription message) returns error? {
        log:printInfo("Received subscription-intent-verification request ", request = message.toString());
        check self.subscribe(message);
    }

    function subscribe(websubhub:VerifiedSubscription message) returns error? {
        log:printInfo("Received subscription request ", request = message);
        string groupName = generateGroupName(message.hubTopic, message.hubCallback);
        kafka:Consumer consumerEp = check createMessageConsumer(message);
        websubhub:HubClient hubClientEp = check new (message);
        _ = start notifySubscriber(hubClientEp, consumerEp);
        // registeredConsumers[groupName] = result;
        error? persistingResult = persistSubscription(message);
        if (persistingResult is error) {
            log:printError("Error occurred while persisting the subscription ", err = persistingResult.message());
        } 
    }

    isolated remote function onUnsubscription(websubhub:Unsubscription message, http:Headers headers)
               returns websubhub:UnsubscriptionAccepted|websubhub:BadUnsubscriptionError|websubhub:InternalUnsubscriptionError|error {
        // check authorize(headers, ["subscribe"]);
        return websubhub:UNSUBSCRIPTION_ACCEPTED;
    }

    isolated remote function onUnsubscriptionValidation(websubhub:Unsubscription message)
                returns websubhub:UnsubscriptionDeniedError? {
        log:printInfo("Received unsubscription-validation request ", request = message.toString());

        string topicName = generateTopicName(message.hubTopic);
        if (!isTopicAvailable(topicName)) {
            return error websubhub:UnsubscriptionDeniedError("Topic [" + message.hubTopic + "] is not registered with the Hub");
        } else {
            string groupName = generateGroupName(message.hubTopic, message.hubCallback);
            if (!isConsumerAvailable(groupName)) {
                return error websubhub:UnsubscriptionDeniedError("Could not find a valid subscriber for Topic [" 
                                + message.hubTopic + "] and Callback [" + message.hubCallback + "]");
            }
        }       
    }

    isolated remote function onUnsubscriptionIntentVerified(websubhub:VerifiedUnsubscription message) {
        log:printInfo("Received unsubscription-intent-verification request ", request = message.toString());
        string groupName = generateGroupName(message.hubTopic, message.hubCallback);
        removeConsumer(groupName);  
        var persistingResult = persistUnsubscription(message);
        if (persistingResult is error) {
            log:printError("Error occurred while persisting the unsubscription ", err = persistingResult.message());
        }  
    }
};
