const amqp = require('amqplib');

const [username, port, side, quantity, price, stock] = process.argv.slice(2);

if (!username || !port || !side || !price) {
  console.log('Usage: node sendOrder.js <username> <port> <BUY|SELL> <quantity> <price> [stock]');
  console.log('Example: node sendOrder.js Alice 5672 BUY 100 10.50');
  process.exit(1);
}

const sideUpper = side.toUpperCase();
if (sideUpper !== 'BUY' && sideUpper !== 'SELL') {
  console.log('Side must be BUY or SELL');
  process.exit(1);
}

async function main() {
  const conn = await amqp.connect(`amqp://localhost:${port}`);
  const ch = await conn.createConfirmChannel();
  await ch.assertExchange('orders', 'fanout', { durable: false });

  const order = {
    user: username,
    side: sideUpper,
    quantity: 100, // fixed at 100 for this assignment
    price: parseFloat(price),
    stock: stock || 'XYZ'
  };

  ch.publish('orders', '', Buffer.from(JSON.stringify(order)));
  await ch.waitForConfirms();
  console.log(`Order sent: ${order.user} ${order.side} ${order.quantity} ${order.stock} @ ${order.price}`);

  await ch.close();
  await conn.close();
}

main().catch((err) => {
  console.error('Error:', err.message);
  process.exit(1);
});
