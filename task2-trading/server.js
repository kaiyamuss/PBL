const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const amqp = require('amqplib');

const AMQP_PORT = process.argv[2] || 5672;
const WEB_PORT = 3001;

const app = express();
app.use(express.static('public'));
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const latest = {};   // latest trade per stock
const history = [];  // most recent trades, newest first
let ch;

function broadcast(obj) {
  const s = JSON.stringify(obj);
  wss.clients.forEach((c) => { if (c.readyState === 1) c.send(s); });
}

wss.on('connection', (ws) => {
  ws.send(JSON.stringify({ type: 'snapshot', latest, history }));

  ws.on('message', (raw) => {
    let d;
    try { d = JSON.parse(raw.toString()); } catch { return; }
    if (d.type !== 'order' || !ch) return;

    const side = String(d.side || '').toUpperCase();
    const price = parseFloat(d.price);
    const user = String(d.user || '').trim();
    const stock = String(d.stock || 'XYZ').trim().toUpperCase();
    if (!user || !['BUY', 'SELL'].includes(side) || isNaN(price)) return;

    ch.publish('orders', '', Buffer.from(JSON.stringify({
      user, side, quantity: 100, price, stock
    })));
  });
});

async function start() {
  const conn = await amqp.connect(`amqp://localhost:${AMQP_PORT}`);
  ch = await conn.createChannel();
  await ch.assertExchange('orders', 'fanout', { durable: false });
  await ch.assertExchange('trades', 'fanout', { durable: false });

  const q = await ch.assertQueue('', { exclusive: true });
  await ch.bindQueue(q.queue, 'trades', '');
  ch.consume(q.queue, (msg) => {
    const t = JSON.parse(msg.content.toString());
    latest[t.stock] = t;
    history.unshift(t);
    if (history.length > 20) history.pop();
    broadcast({ type: 'trade', trade: t });
  }, { noAck: true });

  server.listen(WEB_PORT, () => {
    console.log(`Trading GUI running at http://localhost:${WEB_PORT}`);
  });
}

start().catch((err) => {
  console.error('Could not connect to RabbitMQ:', err.message);
  process.exit(1);
});