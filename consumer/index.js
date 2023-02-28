const { Kafka, logLevel } = require("kafkajs");

module.exports = async () => {
    const kafka = new Kafka({
        clientId: "my-app",
        brokers: ["localhost:9092"],
        logLevel: logLevel.ERROR
    });

    const consumer = kafka.consumer({ groupId: "test-group" });

    await consumer.connect();
    await consumer.subscribe({ topic: "test-topic", fromBeginning: true });

    return consumer
}