const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const amqp = require('amqplib');

const AMQP_PORT = process.argv[2] || 5672;
const WEB_PORT = 3000;

const app = express();
app.use(express.static('public'));
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

let conn;

function send(ws, obj) {
  if (ws.readyState === 1) ws.send(JSON.stringify(obj));
}

wss.on('connection', (ws) => {
  let ch = null;
  let user = null;
  let room = null;

  ws.on('message', async (raw) => {
    let data;
    try { data = JSON.parse(raw.toString()); } catch { return; }

    if (data.type === 'join' && !ch) {
      user = String(data.user || '').trim();
      room = String(data.room || '').trim();
      if (!user || !room) {
        send(ws, { type: 'error', text: 'Username and room are required.' });
        return;
      }
      try {
        ch = await conn.createChannel();
        await ch.assertExchange(room, 'fanout', { durable: false });
        const q = await ch.assertQueue('', { exclusive: true });
        await ch.bindQueue(q.queue, room, '');
        ch.consume(q.queue, (msg) => {
          const m = JSON.parse(msg.content.toString());
          send(ws, { type: 'message', user: m.user, text: m.text });
        }, { noAck: true });
        send(ws, { type: 'joined', user, room });
      } catch (err) {
        ch = null;
        send(ws, { type: 'error', text: 'Could not join room: ' + err.message });
      }
    } else if (data.type === 'message' && ch) {
      const text = String(data.text || '').trim();
      if (!text) return;
      ch.publish(room, '', Buffer.from(JSON.stringify({ user, text })));
    }
  });

  ws.on('close', () => {
    if (ch) ch.close().catch(() => {});
  });
});

amqp.connect(`amqp://localhost:${AMQP_PORT}`)
  .then((c) => {
    conn = c;
    server.listen(WEB_PORT, () => {
      console.log(`Chat GUI running at http://localhost:${WEB_PORT}`);
    });
  })
  .catch((err) => {
    console.error('Could not connect to RabbitMQ:', err.message);
    process.exit(1);
  });
