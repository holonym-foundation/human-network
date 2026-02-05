const { VeraxSdk } = require('@verax-attestation-registry/verax-sdk');
const { privateKeyToAccount } = require('viem/accounts');

const inputs = JSON.parse(process.argv[2]);

async function main() {
    // Attest on Verax (on Linea mainnet)
    try {
        const veraxSdk = new VeraxSdk(
            VeraxSdk.DEFAULT_LINEA_MAINNET, 
            privateKeyToAccount(process.env.ATTESTOR_PRIVATE_KEY).address,
            process.env.ATTESTOR_PRIVATE_KEY
        );
        const portalAddr = "0x66A7bC8eD7062BE723c4AFda780d4fa02F9544F5"
        const schemaId = "0xa5b6504c15d5e0f122247799608a26f1a6f9232213c8a8a0cca2e5aa4c112a60"
        const result = await veraxSdk.portal.attest(
            portalAddr,
            {
                    schemaId,
                    expirationDate: inputs.expiry,
                    subject: inputs.recipient,
                    // (uint256 actionId)
                    attestationData: [{ 
                        actionId: inputs.actionId,
                    }],
            },
            []
        )

        process.stdout.write(result?.transactionHash);
    } catch (err) {
        process.stderr.write(err.message);
    }

}

main()
