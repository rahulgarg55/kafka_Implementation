const { Kafka } = require('kafkajs');
const fs = require('fs');
const csv = require('csv-parser');

// Initialize Kafka producer
const kafka = new Kafka({
  clientId: 'my-app-producer',
  brokers: ['localhost:9092'] // Update with your Kafka broker(s) address
});

const producer = kafka.producer();

async function produceMessages() {
  await producer.connect();

  fs.createReadStream('D:/add_campaign/CampaignSampleFile.csv') // Update with the actual path to your .csv file
    .pipe(csv())
    .on('data', async (row) => {
      await producer.send({
        topic: 'add_campaign',
        messages: [
          { value: JSON.stringify(row) }
        ]
      });
    })
    .on('end', async () => {
      await producer.disconnect();
    });
}

produceMessages().catch(console.error);
