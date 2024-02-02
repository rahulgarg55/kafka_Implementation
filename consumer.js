const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'my-app-consumer', //
  brokers: ['localhost:9092']
});

const consumer = kafka.consumer({ groupId: 'my-group' });

const run = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: 'add_campaign' });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const data=JSON.parse(message.value.toString());
       console.log(data);
    },
  });
};

run().catch(console.error);
