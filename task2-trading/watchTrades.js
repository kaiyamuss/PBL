const amqp = require('amqplib');

const port = process.argv[2] || 5672;

async function main() {
  const conn = await amqp.connect(`amqp://localhost:${port}`);
  const ch = await conn.createChannel();
  await ch.assertExchange('trades', 'fanout', { durable: false });
  const q = await ch.assertQueue('', { exclusive: true });
  await ch.bindQueue(q.queue, 'trades', '');

  console.log('Watching trades...');
  ch.consume(q.queue, (msg) => {
    const t = JSON.parse(msg.content.toString());
    console.log(`TRADE: ${t.stock} ${t.quantity} @ ${t.price} (buyer ${t.buyer}, seller ${t.seller})`);
  }, { noAck: true });
}

main().catch((err) => {
  console.error('Error:', err.message);
  process.exit(1);
});