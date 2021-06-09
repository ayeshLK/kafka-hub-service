import ballerina/websubhub;
import ballerina/io;

public function main() returns error? {
    websubhub:PublisherClient websubHubClientEP = check new("http://localhost:9090/hub",
        auth = {
            tokenUrl: "https://localhost:9443/oauth2/token",
            clientId: "M59Zf4x1H886m2wtD0M1fX69uoga",
            clientSecret: "UN5axp6vsT5FoEXfCUXUH3RtbjIa",
            scopes: ["deregister_topic"],
            clientConfig: {
                secureSocket: {
                    cert: {
                        path: "../resources/client-truststore.jks",
                        password: "wso2carbon"
                    }
                }
            }
        }
    );
    var registrationResponse = websubHubClientEP->deregisterTopic("test");
    io:println("Receieved topic registration result : ", registrationResponse);
}
