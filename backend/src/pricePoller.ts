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

// Websocket logic
ws.on('open', ()=> {
    console.log('WebSocket connection established');
})

ws.on('error', ()=> {
    console.log('Error establishing the websocket connection');
})

// TODO : add batch processing to make it production ready

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
    ) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9)`

    const timeObject = new Date(streamData.E);

    const values = [
        streamData.e,
        streamData.E,
        streamData.s,
        Number(streamData.a),
        Number(streamData.p),
        Number(streamData.q),
        streamData.T,
        streamData.m,
        timeObject
    ]
    async function dataIngestion() {
    try{
        await client.query(insertData, values);
    }
    catch (err) {
        console.error('Error inserting data: ', err);
    }
}
    dataIngestion();
})

ws.on('close', ()=> {
    console.log('WebSocket connection closed');
})