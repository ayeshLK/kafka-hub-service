# Kafka Hub Sample #

* [Apache Kafka](https://kafka.apache.org/) based implementation for [WebSub Hub](https://www.w3.org/TR/websub/#hub).
* This implementation uses [WSO2 Identity Server](https://wso2.com/identity-and-access-management/) as the authentication provider.

## Prerequisites ##

* Ballerina SwanLake SL-Beta 1+
* Apache Kafka 2.7.0

## How to Deploy ##

* Start **Apache Kafka message broker**. Relevent documentation could be found [here](./docs/Kafka-Startup-Guide.md).

* Setup **WSO2 Identity Server**. Use [following guidelines](./docs/Identity-Server-Startup-Guide.md).

* Set up WSO2 Identity Server.

* Build and run the project.

```bash
    bal run .
```

