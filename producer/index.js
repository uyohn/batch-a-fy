const { Kafka } = require("kafkajs");

const main = async () => {
  const kafka = new Kafka({
    clientId: "my-app",
    brokers: ["localhost:9092"],
  });

  const producer = kafka.producer();

  await producer.connect();

  let i = 0;

  // prepare batch of messages with value and some text
  const interval = setInterval(() => {
    const messages = Array(3)
      .fill()
      .map(() => ({
        key: `${++i}`,
        value: `Test message #${i}`,
      }));
    
    // send batch of messages
    producer.send({
      topic: "test-topic",
      messages,
    });

    console.log('Producer sent 3 messages', messages)
  }, 2000)
  
  // stop sending messages after 10 seconds
  // setTimeout(async () => {
  //   clearInterval(interval)
  //   await producer.disconnect();
  // }, 60000)
};

main();