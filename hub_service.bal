import ballerina/websubhub;
import ballerina/log;

listener websubhub:Listener hubListener = new websubhub:Listener(9090);

websubhub:Service hubService = service object {
    remote function onRegisterTopic(websubhub:TopicRegistration message)
                                returns websubhub:TopicRegistrationSuccess|websubhub:TopicRegistrationError {
        log:print("Received topic-registration request ", request = message);

        registerTopic(message);

        return {};
    }

    remote function onDeregisterTopic(websubhub:TopicDeregistration message)
                        returns websubhub:TopicDeregistrationSuccess|websubhub:TopicDeregistrationError {
        log:print("Received topic-deregistration request ", request = message);
        
        deregisterTopic(message);

        return {};
    }

    remote function onUpdateMessage(websubhub:UpdateMessage msg)
               returns websubhub:Acknowledgement|websubhub:UpdateMessageError {        
        log:print("Received content-update request ", request = msg.toString());

        error? errorResponse = publishContent(msg);

        if (errorResponse is websubhub:UpdateMessageError) {
            return errorResponse;
        } else if (errorResponse is error) {
            log:printError("Error occurred while publishing the content ", errorMessage = errorResponse.message());
            return error websubhub:UpdateMessageError(errorResponse.message());
        } else {
            return {};
        }
    }
    
    remote function onSubscription(websubhub:Subscription msg)
                returns websubhub:SubscriptionAccepted {
        log:print("Received subscription-request ", request = msg.toString());
        
        return {};
    }

    remote function onSubscriptionValidation(websubhub:Subscription msg)
                returns websubhub:SubscriptionDeniedError? {
        log:print("Received subscription-validation request ", request = msg.toString());

        var result = validateSubscription(msg);

        log:printError("Received validation response ", err = result);
        return result;
    }

    remote function onSubscriptionIntentVerified(websubhub:VerifiedSubscription msg) {
        log:print("Received subscription-intent-verification request ", request = msg.toString());

        var result = subscribe(msg);
    }

    remote function onUnsubscription(websubhub:Unsubscription msg)
               returns websubhub:UnsubscriptionAccepted|websubhub:BadUnsubscriptionError|websubhub:InternalUnsubscriptionError {
        log:print("Received unsubscription request ", request = msg.toString());

        return {};
    }

    remote function onUnsubscriptionValidation(websubhub:Unsubscription msg)
                returns websubhub:UnsubscriptionDeniedError? {
        log:print("Received unsubscription-validation request ", request = msg.toString());

        return validateUnsubscription(msg);
    }

    remote function onUnsubscriptionIntentVerified(websubhub:VerifiedUnsubscription msg){
        log:print("Received unsubscription-intent-verification request ", request = msg.toString());

        var result = unsubscribe(msg);
    }
};