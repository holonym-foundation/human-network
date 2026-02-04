const util = require('node:util');
const exec = util.promisify(require('node:child_process').exec);

// node_idx is the index of the peer in the list of peers
const node_idx = process.argv[2]
const rpc_url = process.argv[3]

if (!node_idx) {
    console.error('Missing argument');
    process.exit(1);
}

async function main() {
    const command = `cast abi-decode "getPeers()((address,string,string,string,string,string)[])" $(cast call --rpc-url ${rpc_url} --private-key 0x2a871d0798f97d79848a013d4936a73bf4cc922c825d33c1cf7073dff6d409c6 0x5FbDB2315678afecb367f032d93F642f64180aa3 "getPeers()")`;
    const { stdout: peerInfo, stderr } = await exec(command);
    if (stderr) throw new Error(stderr);
    const peerId = peerInfo.split('),')[node_idx].split(', ')[1].replaceAll('"', '');
    process.stdout.write(peerId);
}

main()
    .then(() => {
        process.exit(0);
    })
    .catch((err) => {
        console.error(err);
        process.exit(1);
    });
