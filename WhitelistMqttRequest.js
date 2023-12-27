import MqttRequest from 'mqtt-request';
MqttRequest.timeout = 5000;

/// We're using the npm package `mqtt-request` throughout our project,
/// but there are some problems with it. As soon as we're using it in a service,
/// it will respond to every MQTT message it receives. This is not what we want,
/// as we only want to log the requests "silently".
/// Therefore, this class is a workaround to only whitelist certain topics for
/// `mqtt-request` to respond to.

export default class WhitelistMqttRequest extends MqttRequest.default {
    constructor(mqttClient, whitelist) {
        super(mqttClient);
        this._whitelist = whitelist;
    }

    _handleMessage(topic, payload) {
        for (let entry of this._whitelist) {
            if (topic.startsWith(`${entry}/@request/`)) {
                super._handleMessage(topic, payload);
                return
            }
        }
    }
}