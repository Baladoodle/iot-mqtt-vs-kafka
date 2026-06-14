#!/usr/bin/env node
// Diag: publish N=1000 with sequential id, count what one subscriber gets.
// Run: node scripts/diag-mqtt.js <broker_url>
//   e.g. node scripts/diag-mqtt.js mqtt://localhost:1883
//
// This tells us if the broker is duplicating (received > 1000) or not.

const mqtt = require('/home/baladoodle/Baladoodle/Fakultet/IoTS/Proj-2/services/storage/node_modules/mqtt');

const URL = process.argv[2] || 'mqtt://localhost:1883';
const N = 1000;
const TOPIC = 'diag/test/device_1';

const received = [];   // collected message payloads (id field)

const client = mqtt.connect(URL, { clientId: 'diag-sub', protocolVersion: 5 });

client.on('connect', () => {
    console.log('subscriber connected');
    client.subscribe(TOPIC, { qos: 0 }, (err) => {
        if (err) { console.error('subscribe err', err); process.exit(1); }
        console.log('subscribed to', TOPIC);
        // Wait briefly so subscription is established, then start publishing
        setTimeout(startPublish, 500);
    });
});

client.on('message', (topic, payload) => {
    try {
        const obj = JSON.parse(payload.toString());
        received.push(obj.id);
    } catch (e) {
        received.push('PARSE_ERR');
    }
});

async function startPublish() {
    const pub = mqtt.connect(URL, { clientId: 'diag-pub', protocolVersion: 5, clean: true });
    await new Promise((res, rej) => {
        pub.on('connect', () => { console.log('publisher connected'); res(); });
        pub.on('error', rej);
    });

    for (let i = 0; i < N; i++) {
        const msg = JSON.stringify({ id: i, ts: Date.now() });
        pub.publish(TOPIC, msg, { qos: 0 });
    }
    console.log(`published ${N} messages, waiting for delivery...`);

    // Disconnect publisher after a short delay
    setTimeout(() => pub.end(), 500);
    // Give subscriber time to drain
    setTimeout(report, 3000);
}

function report() {
    const total = received.length;
    const uniqueIds = new Set(received).size;
    const dupes = total - uniqueIds;
    const sorted = [...received].sort((a, b) => a - b);

    // Find the first duplicated id
    let firstDupe = null;
    if (dupes > 0) {
        const seen = new Set();
        for (const id of sorted) {
            if (seen.has(id)) { firstDupe = id; break; }
            seen.add(id);
        }
    }

    console.log('--- REPORT ---');
    console.log(`published:    ${N}`);
    console.log(`received:     ${total}`);
    console.log(`unique ids:   ${uniqueIds}`);
    console.log(`duplicates:   ${dupes}`);
    console.log(`first dupe id: ${firstDupe}`);
    if (sorted.length <= 20) {
        console.log(`received ids: ${sorted.join(',')}`);
    } else {
        console.log(`first 10 ids: ${sorted.slice(0, 10).join(',')}`);
        console.log(`last 10 ids:  ${sorted.slice(-10).join(',')}`);
    }
    client.end();
    process.exit(0);
}
