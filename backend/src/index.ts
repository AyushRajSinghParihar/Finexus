import express, { json, type Request, type Response } from "express";
import pg from "pg";

const app = express();

const client = new pg.Client();

await client.connect();

const durations = {
    "1m" : "1 minute",
    "5m" : "5 minute",
    "1hr" : "1 hour",
    "1d" : "1 day",
    "1w" : "1 week",
    "1mo" : "1 month"
}

app.get("/candles", async (req: Request, res: Response) => {
    const query = req.query;
    const asset = query.asset as string | undefined;
    const durationKey = query.duration as keyof typeof durations | undefined;
    const duration = durations[query.duration as keyof typeof durations];
    const startTime = query.startTime as string | undefined;
    const endTime = query.endTime as string | undefined;

    if(!duration){
        return res.status(404).json({message: "Invalid duration"});
    }
    if(!asset){
        return res.status(404).json({message: "Asset not found"});
    }
    if(!startTime || !endTime){
        return res.status(404).json({message: "Please provide startTime and endTime"});
    }
    const candlesQuery = await client.query(
        `SELECT time_bucket('$1', time) AS bucket, 
        first(price, time) AS open,
        last(price, time) AS close,
        MIN(price) AS low,
        MAX(price) AS high,
        sum(quantity) AS volume
        FROM master_agg_trade
        WHERE symbol = $2
        AND time BETWEEN $3 AND $4`,
        [duration, asset, startTime, endTime]
    );

    res.json(candlesQuery.rows);
});

app.listen(3000, () => {
    console.log("The server is running in port 3000");
});