const amqp = require('amqplib');

const port = process.argv[2] || 5672;

// One order book per stock: { XYZ: { buys: [], sells: [] }, ... }
const books = {};

function getBook(stock) {
  if (!books[stock]) books[stock] = { buys: [], sells: [] };
  return books[stock];
}

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
    order.stock = order.stock || 'XYZ';
    console.log(`Received: ${order.user} ${order.side} ${order.quantity} ${order.stock} @ ${order.price}`);
    handleOrder(order, ch);
  }, { noAck: true });
}

function handleOrder(order, ch) {
  const book = getBook(order.stock);
  const isBuy = order.side === 'BUY';
  const oppositeBook = isBuy ? book.sells : book.buys;

  let bestIndex = -1;
  for (let i = 0; i < oppositeBook.length; i++) {
    const other = oppositeBook[i];
    const acceptable = isBuy ? other.price <= order.price : other.price >= order.price;
    if (!acceptable) continue;
    if (bestIndex === -1) {
      bestIndex = i;
    } else {
      const best = oppositeBook[bestIndex];
      if (isBuy ? other.price < best.price : other.price > best.price) bestIndex = i;
    }
  }

  if (bestIndex === -1) {
    (isBuy ? book.buys : book.sells).push(order);
    console.log(`  No match. Added to ${order.stock} book. (Buys: ${book.buys.length}, Sells: ${book.sells.length})`);
    return;
  }

  const resting = oppositeBook.splice(bestIndex, 1)[0];
  const trade = {
    stock: order.stock,
    price: resting.price,
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