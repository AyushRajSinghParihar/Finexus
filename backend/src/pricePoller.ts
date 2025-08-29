import WebSocket = require("ws");
import pg = require("pg");
import redis = require("redis");

const publisher = redis.createClient({
  url: "redis://localhost:6379",
});

await publisher.connect();

const ws = new WebSocket(
  "wss://stream.binance.com:9443/stream?streams=btcusdt@trade",
);
const pool = new pg.Pool({
  host: "localhost",
  port: 5432,
  user: "postgres",
  password: "password",
  database: "postgres",
});

await pool.connect();

let buffer: any[] = [];
const BATCH_SIZE = 500;
const FLUSH_INTERVAL = 500;
async function flushBuffer() {
  if (buffer.length === 0) return;
  const batch = buffer;
  buffer = [];

  const values: any[] = [];
  const placeholders: string[] = [];

  batch.forEach((trade, i) => {
    const baseIndex = i * 9;
    placeholders.push(`($${baseIndex + 1}, $${baseIndex + 2}, $${baseIndex + 3}, 
                                $${baseIndex + 4},$${baseIndex + 5},$${baseIndex + 6},
                                $${baseIndex + 7},$${baseIndex + 8},$${baseIndex + 9})`);
    values.push(
      trade.e,
      trade.E,
      trade.s,
      Number(trade.t),
      Number(trade.p),
      Number(trade.q),
      trade.T,
      trade.m,
      new Date(trade.E),
    );
  });
  const query = `INSERT INTO master_agg_trade(
            event_type, 
            event_time, 
            symbol, 
            aggregate_trade_id, 
            price,
            quantity, 
            trade_time, 
            market_maker,
            time
        ) VALUES ${placeholders.join(",")}`;

  try {
    await pool.query(query, values);
    console.log(`Inserted ${batch.length} rows`);
  } catch (err) {
    console.error(`Error received while inserting batch: ${err}`);
  }
}

setInterval(flushBuffer, FLUSH_INTERVAL);

// Websocket logic
ws.on("open", () => {
  console.log("WebSocket connection established");
});

ws.on("error", () => {
  console.log("Error establishing the websocket connection");
});

ws.on("message", async (data) => {
  const parsedData = JSON.parse(data.toString());
  const trade = parsedData.data;

  buffer.push(trade);
  if (buffer.length >= BATCH_SIZE) {
    await flushBuffer();
  }
  await publisher.publish("trade:btcusdt", JSON.stringify(trade));
});

ws.on("close", () => {
  console.log("WebSocket connection closed");
});
