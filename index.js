import * as mqtt from "mqtt"
import PGClient from "pg-native"

const db = new PGClient()
db.connectSync(process.env.CONNECTION_STRING)

const client = mqtt.connect(process.env.BROKER_URL)

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
    payload = JSON.stringify(payload)

    try {
        db.querySync("insert into public.logs (topic, payload) values ($1, $2)", [topic, payload])
        console.log(`Logged message on topic ${topic}`)
    } catch (e) {
        console.log(e)
    }
});

client.on("error", (error) => {
    console.error("Error connecting to the broker:", error);
});

process.on('SIGINT', () => {
    client.end();
    console.log('Disconnected from MQTT broker');
    process.exit();
});
