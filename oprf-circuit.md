# OPRF template (trusting relay node for sybil resistance)


## Inputs
- secret: field, the secret data that identifies a user e.g. SSN
- mask: field, a random element from the prime field of the curve's subgroup order
- relay_signature: an Eddsa signature of the network's input / output pair. https://github.com/iden3/circomlib/blob/master/circuits/eddsa.circom
- relay_public_key: field[2], the public key of the relay node
- network_output: field[2], the point output by the network that is part of the tuple signed by the relay node

## Outputs
- output: field, the output of the OPRF protocol

## Constraints
- hashed_to_curve <== H(secret) where H is a hash to curve algorithm that is deterministic, e.g. Elligator2
- network_input <== mask * hashed_to_curve
- relay_signature is a valid signature of an (network_input, network_output), signed by the relay node's private key
- maskinv <== 1/mask
- output <== network_output * maskinv




