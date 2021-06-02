import ballerina/websubhub;
import ballerina/log;
import ballerina/http;

websubhub:Service hubService = service object {
    remote function onRegisterTopic(websubhub:TopicRegistration message, http:Headers headers)
                                returns websubhub:TopicRegistrationSuccess|websubhub:TopicRegistrationError|error {
        check authorize(headers, ["register_topic"]);
        registerTopic(message);
        return websubhub:TOPIC_REGISTRATION_SUCCESS;
    }

    remote function onDeregisterTopic(websubhub:TopicDeregistration message, http:Headers headers)
                        returns websubhub:TopicDeregistrationSuccess|websubhub:TopicDeregistrationError|error {
        check authorize(headers, ["deregister_topic"]);
        deregisterTopic(message);
        return websubhub:TOPIC_DEREGISTRATION_SUCCESS;
    }

    remote function onUpdateMessage(websubhub:UpdateMessage msg, http:Headers headers)
               returns websubhub:Acknowledgement|websubhub:UpdateMessageError|error {  
        check authorize(headers, ["update_content"]);
        check self.updateMessage(msg);
        return websubhub:ACKNOWLEDGEMENT;
    }

    function updateMessage(websubhub:UpdateMessage msg) returns websubhub:UpdateMessageError? {
        log:printInfo("Received content-update request ", request = msg.toString());
        error? errorResponse = publishContent(msg);
        if (errorResponse is websubhub:UpdateMessageError) {
            return errorResponse;
        } else if (errorResponse is error) {
            log:printError("Error occurred while publishing the content ", errorMessage = errorResponse.message());
            return error websubhub:UpdateMessageError(errorResponse.message());
        }
    }
    
    remote function onSubscription(websubhub:Subscription message, http:Headers headers)
                returns websubhub:SubscriptionAccepted|websubhub:BadSubscriptionError|error {
        check authorize(headers, ["subscribe"]);
        return websubhub:SUBSCRIPTION_ACCEPTED;
    }

    remote function onSubscriptionValidation(websubhub:Subscription message)
                returns websubhub:SubscriptionDeniedError? {
        log:printInfo("Received subscription-validation request ", request = message.toString());

        string topicName = generateTopicName(message.hubTopic);
        if (!registeredTopics.hasKey(topicName)) {
            return error websubhub:SubscriptionDeniedError("Topic [" + message.hubTopic + "] is not registered with the Hub");
        }
    }

    remote function onSubscriptionIntentVerified(websubhub:VerifiedSubscription message) returns error? {
        log:printInfo("Received subscription-intent-verification request ", request = message.toString());
        check subscribe(message);
    }

    remote function onUnsubscription(websubhub:Unsubscription message, http:Headers headers)
               returns websubhub:UnsubscriptionAccepted|websubhub:BadUnsubscriptionError|websubhub:InternalUnsubscriptionError|error {
        check authorize(headers, ["subscribe"]);
        return websubhub:UNSUBSCRIPTION_ACCEPTED;
    }

    remote function onUnsubscriptionValidation(websubhub:Unsubscription message)
                returns websubhub:UnsubscriptionDeniedError? {
        log:printInfo("Received unsubscription-validation request ", request = message.toString());

        string topicName = generateTopicName(message.hubTopic);
        if (!registeredTopics.hasKey(topicName)) {
            return error websubhub:UnsubscriptionDeniedError("Topic [" + message.hubTopic + "] is not registered with the Hub");
        } else {
            string groupName = generateGroupName(message.hubTopic, message.hubCallback);
            if (!registeredConsumers.hasKey(groupName)) {
                return error websubhub:UnsubscriptionDeniedError("Could not find a valid subscriber for Topic [" 
                                + message.hubTopic + "] and Callback [" + message.hubCallback + "]");
            }
        }       
    }

    remote function onUnsubscriptionIntentVerified(websubhub:VerifiedUnsubscription message) {
        log:printInfo("Received unsubscription-intent-verification request ", request = message.toString());

        string groupName = generateGroupName(message.hubTopic, message.hubCallback);
        var registeredConsumer = registeredConsumers[groupName];
        if (registeredConsumer is future<error?>) {
             _ = registeredConsumer.cancel();
            var result = registeredConsumers.remove(groupName);
        }  

        var persistingResult = persistUnsubscription(message);
        if (persistingResult is error) {
            log:printError("Error occurred while persisting the unsubscription ", err = persistingResult.message());
        }  
    }
};
