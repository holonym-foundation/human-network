## Environment variables

```bash
SIGNER_ENV=prod
HUMAN_RPC_URL=<HUMAN_RELAY_NODE_MAINNET_ALPHA_URL>
HUMAN_SIGNER_PRIVATE_KEY=<YOUR_WHITELISTED_PRIVATE_KEY ex: 0x4a54...>
SIGNER_PORT=<DESIRED_PORT_DEFAULT_IS_3030>
ALLOWED_METHODS=<ADD_HERE_THE_ALLOWED_METHODS>
# If you want to enable rate limiting, default is true
RATE_LIMIT_ENABLED=true
# This is the maximum number of requests allowed in the time interval, default is 100
RATE_LIMIT_NUM_REQUESTS=100
# in seconds, default is 1 day
RATE_LIMIT_TIME_INTERVAL=86400
# Datadog config. Only include these if you want to send logs to Datadog
DD_API_KEY=<YOUR_DD_API_KEY>
DD_SERVICE=<YOUR_DD_SERVICE>
DD_ENV=<YOUR_DD_ENV>
DD_SITE=<DD_SITE ex: us.datadoghq.com>
```
