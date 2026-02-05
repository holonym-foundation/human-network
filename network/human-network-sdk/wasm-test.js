const wasm = require('./pkg/cjs/human_network_sdk');

async function runRequest(i) {
    try {
        const result = await wasm.request_from_signer(`usr:${i}`, "OPRFSecp256k1", "http://127.0.0.1:3030");
        console.log(`Request ${i} succeeded:`, result);
    } catch (err) {
        console.error(`Request ${i} failed:`, err);
    } finally {
        console.log(`Request ${i} completed`);
    }
}

async function runParallelRequests() {
    const requests = [];
    for (let i = 1; i <= 101; i++) {
        requests.push(runRequest(i));
    }
    await Promise.all(requests);
    console.log("All requests completed");
}

runParallelRequests();