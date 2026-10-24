const amqp = require('amqplib');

const port = process.argv[2] || 5672;

// Order book: orders waiting for a match
const buyBook = [];   // BUY orders
const sellBook = [];  // SELL orders

async function main() {
  const conn = await amqp.connect(`amqp://localhost:${port}`);
  const ch = await conn.createChannel();

  await ch.assertExchange('orders', 'fanout', { durable: false });
  await ch.assertExchange('trades', 'fanout', { durable: false });

  const q = await ch.assertQueue('exchange_orders', { durable: false });
  await ch.bindQueue(q.queue, 'orders', '');

  console.log('Exchange running. Waiting for orders...');

  ch.consume(q.queue, (msg) => {
    const order = JSON.parse(msg.content.toString());
    console.log(`Received: ${order.user} ${order.side} ${order.quantity} @ ${order.price}`);
    handleOrder(order, ch);
  }, { noAck: true });
}

function handleOrder(order, ch) {
  const isBuy = order.side === 'BUY';
  const oppositeBook = isBuy ? sellBook : buyBook;

  // Find the best acceptable opposite order
  let bestIndex = -1;
  for (let i = 0; i < oppositeBook.length; i++) {
    const other = oppositeBook[i];
    const acceptable = isBuy ? other.price <= order.price : other.price >= order.price;
    if (!acceptable) continue;
    if (bestIndex === -1) {
      bestIndex = i;
    } else {
      const best = oppositeBook[bestIndex];
      // Buyer wants the lowest sell price; seller wants the highest buy price
      if (isBuy ? other.price < best.price : other.price > best.price) bestIndex = i;
    }
  }

  if (bestIndex === -1) {
    // No match: add to the order book
    (isBuy ? buyBook : sellBook).push(order);
    console.log(`  No match. Added to book. (Buys: ${buyBook.length}, Sells: ${sellBook.length})`);
    return;
  }

  // Match found: remove the resting order and publish the trade
  const resting = oppositeBook.splice(bestIndex, 1)[0];
  const trade = {
    stock: order.stock,
    price: resting.price, // trade at the price of the order that was waiting
    quantity: order.quantity,
    buyer: isBuy ? order.user : resting.user,
    seller: isBuy ? resting.user : order.user,
    time: new Date().toISOString()
  };
  ch.publish('trades', '', Buffer.from(JSON.stringify(trade)));
  console.log(`  TRADE: ${trade.stock} ${trade.quantity} @ ${trade.price} (buyer ${trade.buyer}, seller ${trade.seller})`);
}

main().catch((err) => {
  console.error('Could not connect to RabbitMQ:', err.message);
  process.exit(1);
});