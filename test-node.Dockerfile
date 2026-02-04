# Dockerfile to be used with the docker compose file for testing

# TODO: Change the name of the human node image (called "human" 
# here) to the correct value once we push it to the registry.
FROM human AS human

FROM ghcr.io/foundry-rs/foundry AS foundry_builder

FROM node:22-bullseye-slim

COPY --from=human / /

COPY --from=foundry_builder /usr/local/bin/cast /usr/local/bin/cast

COPY ./scripts/test/ /scripts/test/
