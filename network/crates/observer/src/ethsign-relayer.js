/**
 * Relayer script to issue EthSign attestation
 */
const { createWalletClient, http } = require('viem');
const { privateKeyToAccount } = require('viem/accounts');
const { optimism } = require('viem/chains')
const {
    SignProtocolClient,
    SpMode,
    EvmChains,
} = require('@ethsign/sp-sdk');
require('dotenv').config();

/**
 * @typedef {Object} Inputs
 * @property {string} recipient
 * @property {string} actionId
 * @property {string} actionNullifier
 * @property {number} expiry
 */

const inputs = JSON.parse(process.argv[2]);

async function main() {
    try {
        // Attest on EthSign contract
        const account = privateKeyToAccount(process.env.ATTESTOR_PRIVATE_KEY);
        const walletClient = createWalletClient({
            chain: optimism,
            transport: http(process.env.OP_RPC_URL),
            account,
        });
        const client = new SignProtocolClient(SpMode.OnChain, {
            chain: EvmChains.optimism,
            account,
            walletClient
        });
        const result = await client.createAttestation({
            schemaId: "0x8",
            data: {
                actionId: inputs.actionId
            },
            recipients: [inputs.recipient],
            indexingValue: inputs.actionNullifier,
            // validUntil is expressed as seconds since UNIX epoch
            validUntil: inputs.expiry,
        });

        // if it is successful return the tx hash
        process.stdout.write(result.txHash);
    } catch (err) {
        console.error(err)
        process.stderr.write(err.reason ?? err.message);
    }
}

main()
