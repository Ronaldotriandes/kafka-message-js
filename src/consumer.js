const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'kafka-app-test',
  brokers: ['localhost:9092']
});

const consumer = kafka.consumer({ groupId: 'message-group' });

const start = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: 'messages', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        topic,
        partition,
        value: message.value.toString(),
        timestamp: new Date().toISOString()
      });
    },
  });
};

start().catch(console.error);
