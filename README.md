# MQTT Bridge
This extends the local MQTT broker to send topics to and collect from my Hive cloud MQTT broker.

Serial2 provides the topic stream to and from the local MQTT Broker. Local MQTT Broker decides what gets forwarded to the bridge fromn the topic patten.

Hive subscribe is # so everything sent to Hive comes in. Therefore  anything published from the bridge to Hive will echo back. A topic screener rejects these echos, everything else is published locally.

Operationally only low frequency stuff gets to Hive.

An earlier version had the broker and bridge in one and used two different MQTT libraries. It worked, but could block for unusual lengths of time during reconnects and time out MQTT sessions. 
