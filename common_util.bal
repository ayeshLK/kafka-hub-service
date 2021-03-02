import ballerinax/kafka;

const string TOPIC_PREFIX = "topic_";
const string GROUP_PREFIX = "consumer_group_";

public type Subscriber record {|
    string hub;
    string hubMode;
    string hubCallback;
    string hubTopic;
    string? hubLeaseSeconds = ();
    string? hubSecret = ();
    boolean verificationSuccess;
|};

isolated function generateTopicName(string topic) returns string {
    return TOPIC_PREFIX + getStringHash(topic).toString();
}

isolated function generateGroupId(string topic, string callbackUrl) returns string {
    string idValue = topic + ":" + callbackUrl;
    return GROUP_PREFIX + getStringHash(idValue).toString();
}

isolated function getStringHash(string value) returns int {
    int hashCount = 0;
    int index = 0;
    while (index < value.length()) {
        hashCount += value.getCodePoint(index);
        index += 1;
    }
    return hashCount;
}

function getConsumer(string[] topics, string consumerGroupId, boolean autoCommit = true) returns kafka:Consumer|error {
    kafka:ConsumerConfiguration consumerConfiguration = {
        bootstrapServers: "localhost:9092",
        groupId: consumerGroupId,
        offsetReset: "latest",
        topics: topics,
        autoCommit: autoCommit
    };

    return check new (consumerConfiguration);
}