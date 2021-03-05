import ballerina/websubhub;
import ballerina/crypto;
import ballerina/log;
import ballerinax/kafka;
import ballerina/http;
import ballerina/jwt;

map<string> registeredTopics = {};
map<future<error?>> registeredConsumers = {};

kafka:ProducerConfiguration mainProducerConfig = {
    bootstrapServers: "localhost:9092",
    clientId: "main-producer",
    acks: "1",
    retryCount: 3
};

kafka:Producer mainProducer = checkpanic new (mainProducerConfig);

listener websubhub:Listener hubListener = new websubhub:Listener(9090);

http:JwtValidatorConfig config = {
    audience: "[<client-id1>, <client-id2>]",
    signatureConfig: {
        trustStoreConfig: {
            trustStore: {
                path: "<trust-store-path>",
                password: "<trust-store-password>"
            },
            certAlias: "<trust-store-alias>"
        }
    }
};
http:ListenerJwtAuthHandler handler = new(config);

websubhub:Service hubService = service object {
    remote function onRegisterTopic(websubhub:TopicRegistration message, http:Headers headers)
                                returns websubhub:TopicRegistrationSuccess|websubhub:TopicRegistrationError {
        var authHeader = headers.getHeader(http:AUTH_HEADER);
        if (authHeader is string) {
            jwt:Payload|http:Unauthorized auth = handler.authenticate(authHeader);
            if (auth is jwt:Payload && validateJwt(auth, ["register_topic"])) {
                log:print("Received topic-registration request ", request = message);
                registerTopic(message);
                return {};
            } else {
                log:printError("Authentication credentials invalid");
                return error websubhub:TopicRegistrationError("Not authorized");   
            }
        } else {
            log:printError("Authorization header not found");
            return error websubhub:TopicRegistrationError("Not authorized");
        }
    }

    remote function onDeregisterTopic(websubhub:TopicDeregistration message)
                        returns websubhub:TopicDeregistrationSuccess|websubhub:TopicDeregistrationError {
        log:print("Received topic-deregistration request ", request = message);
        
        string topicId = crypto:hashSha1(message.topic.toBytes()).toBase64();
        if (registeredTopics.hasKey(topicId)) {
            string deletedTopic = registeredTopics.remove(topicId);
        }

        var persistingResult = persistTopicDeregistration(message);
        if (persistingResult is error) {
            log:printError("Error occurred while persisting the topic-deregistration ", err = persistingResult);
        }

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
    
    remote function onSubscription(websubhub:Subscription message)
                returns websubhub:SubscriptionAccepted {
        log:print("Received subscription-request ", request = message.toString());
        
        return {};
    }

    remote function onSubscriptionValidation(websubhub:Subscription message)
                returns websubhub:SubscriptionDeniedError? {
        log:print("Received subscription-validation request ", request = message.toString());

        string topicId = crypto:hashSha1(message.hubTopic.toBytes()).toBase64();
        if (!registeredTopics.hasKey(topicId)) {
            return error websubhub:SubscriptionDeniedError("Topic [" + message.hubTopic + "] is not registered with the Hub");
        }
    }

    remote function onSubscriptionIntentVerified(websubhub:VerifiedSubscription message) {
        log:print("Received subscription-intent-verification request ", request = message.toString());

        var result = subscribe(message);
    }

    remote function onUnsubscription(websubhub:Unsubscription message)
               returns websubhub:UnsubscriptionAccepted|websubhub:BadUnsubscriptionError|websubhub:InternalUnsubscriptionError {
        log:print("Received unsubscription request ", request = message.toString());

        return {};
    }

    remote function onUnsubscriptionValidation(websubhub:Unsubscription message)
                returns websubhub:UnsubscriptionDeniedError? {
        log:print("Received unsubscription-validation request ", request = message.toString());

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

    remote function onUnsubscriptionIntentVerified(websubhub:VerifiedUnsubscription message){
        log:print("Received unsubscription-intent-verification request ", request = message.toString());

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
};