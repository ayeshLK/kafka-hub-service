import ballerina/regex;

isolated function generateTopicName(string topic) returns string {
    return nomalizeString(topic);
}

isolated function generateGroupName(string topic, string callbackUrl) returns string {
    string idValue = topic + ":::" + callbackUrl;
    return nomalizeString(idValue);
}

isolated function nomalizeString(string baseString) returns string {
    return regex:replaceAll(baseString, "[^a-zA-Z0-9]", "_");
}

isolated class Switch {
    private boolean switch;

    isolated function init(boolean switch = true) {
        self.switch = switch;
    }

    isolated function isOpen() returns boolean {
        lock {
            return self.switch;
        }
    }

    isolated function close() {
        lock {
            self.switch = false;
        }
    }
}
