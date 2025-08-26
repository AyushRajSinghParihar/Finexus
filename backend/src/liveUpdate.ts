import WebSocket = require("ws");
import pg = require("pg");

const ws = new WebSocket("wss://stream.binance.com:9443/stream?streams=btcusdt@trade");
const client = new pg.Client({
    host: "localhost",
    port: 5432,
    user: "postgres",
    password: "password",
    database: "postgres"
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
    const insertData = `INSERT INTO master_agg_trade(
        event_type, 
        event_time, 
        symbol, 
        aggregate_trade_id, 
        price, 
        quantity, 
        trade_time, 
        market_maker,
        time
    ) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING *`

    const timeObject = new Date(streamData.E);

    const values = [
        streamData.e,
        streamData.E,
        streamData.s,
        streamData.a,
        streamData.p,
        streamData.q,
        streamData.T,
        streamData.m,
        timeObject
    ]
    async function dataIngestion() {
        await client.query(insertData, values);
    }
    dataIngestion();
})

ws.on('close', ()=> {
    console.log('WebSocket connection closed');
})

