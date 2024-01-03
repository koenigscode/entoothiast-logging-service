import * as mqtt from "mqtt"
import WhitelistMqttRequest from "./WhitelistMqttRequest.js";
import PGClient from "pg-native"
import jwt from "jsonwebtoken"

const db = new PGClient()
db.connectSync(process.env.CONNECTION_STRING)

const client = mqtt.connect(process.env.BROKER_URL)

export const mqttReq = new WhitelistMqttRequest(client, ["v1/logging/read"]);


console.log(`Broker URL: ${process.env.BROKER_URL}`)

client.on("connect", async () => {
    console.log("logging-service connected to broker")
    client.subscribe("#", (err) => {
        if (err) {
            console.log(err)
        }
    });
});


client.on('message', (topic, payload) => {
    payload = JSON.parse(payload)
    delete payload.password
    if (payload.hasOwnProperty("logs")) {
        payload.logs = "[Truncated]"
    }
    payload = JSON.stringify(payload)

    try {
        db.querySync("insert into public.logs (topic, payload) values ($1, $2)", [topic, payload])
        console.log(`Logged: topic: "${topic}" payload: "${payload}"`)
    } catch (e) {
        console.log(e)
    }
});


mqttReq.response("v1/logging/read", payload => {
    payload = JSON.parse(payload)
    const token = jwt.decode(payload.token, process.env.JWT_SECRET)

    if (!token) {
        return JSON.stringify({ httpStatus: 401, message: "Unauthorized" })
    }

    if (token.role !== "admin") {
        return JSON.stringify({ httpStatus: 403, message: "Forbidden" })
    }

    let query = "select id, timestamp, topic, payload from public.logs order by id desc";
    let params = [];

    if (payload.offset) {
        if (parseInt(payload.offset) < 0) {
            return JSON.stringify({ httpStatus: 400, message: "Offset must be >= 0 or unset" })
        }
        query += " offset $1";
        params.push(payload.offset);
    }

    if (payload.limit) {
        if (parseInt(payload.limit) < 1) {
            return JSON.stringify({ httpStatus: 400, message: "Limit must be >=1 or unset" })
        }
        query += " limit $" + (params.length + 1);
        params.push(payload.limit);
    }

    try {
        const logs = db.querySync(query, params);
        return JSON.stringify({ httpStatus: 200, logs });
    } catch {
        return JSON.stringify({ httpStatus: 500, message: "Some error occurred" });
    }
})

client.on("error", (error) => {
    console.error("Error connecting to the broker:", error);
});

process.on('SIGINT', () => {
    client.end();
    console.log('Disconnected from MQTT broker');
    process.exit();
});
