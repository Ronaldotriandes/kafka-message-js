const express = require('express');
const { Kafka } = require('kafkajs');

const app = express();
app.use(express.json());

const kafka = new Kafka({
  clientId: 'kafka-app-test',
  brokers: ['localhost:9092']
});

const producer = kafka.producer();

app.post('/messages', async (req, res) => {
  try {
    const { message } = req.body;
    await producer.connect();
    await producer.send({
      topic: 'messages',
      messages: [{ value: message }],
    });
    res.json({ success: true, message: 'Message sent successfully!' });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

const start = async () => {
  await producer.connect();
  app.listen(3000, () => console.log('Producer running on port 3000'));
};

start();
