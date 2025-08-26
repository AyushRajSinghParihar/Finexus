import WebSocket = require("ws");
import pg = require("pg");

const ws = new WebSocket("wss://stream.binance.com:9443/stream?streams=btcusdt@trade");
const client = new pg.Client({
    host: "localhost",
    port: 5432,
    user: "postgres",
    password: "password",
    database: "timescaledb"
})

client.connect();

async function initializeConnection() {
    await client.connect();
};
// Websocket logic
ws.on('open', ()=> {
    console.log('WebSocket connection established');
})

ws.on('error', ()=> {
    console.log('Error establishing the websocket connection');
})

ws.on('message', (data)=> {
    const parsedData = JSON.parse(data.toString());
    // Handle the parsed data
    const streamData = parsedData.data;
    console.log(streamData);
})

ws.on('close', ()=> {
    console.log('WebSocket connection closed');
})

