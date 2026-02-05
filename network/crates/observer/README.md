# Observer

The Observer is a primary component in the Clean Hands stack. In the Clean Hands flow, a user generates a ZKP to prove they have passed sanctions checks, and this ZKP outputs the ciphertext of the user's personal identifiable information (PII) and the user's associated blockchain address. This PII can be used to identify the user if their associated blockchain address is suspected of money laundering. The Observer's role in this system is to verify ZKPs, issue attestations to users with valid ZKPs, and to store the public outputs of these ZKPs so that the ciphertext can be decrypted, with the help of Human network, if necessary.

### Run

First, make sure you are running a MongoDB service.

Set environment variables. `SUI_PRIVATE_KEY` should be the output of the `sui keytool export` command.

    export NODEJS_SCRIPTS_PATH=./crates/observer/src
    export CIRCUIT_DIR=./crates/observer/circuits
    # You might want to modify the following
    export MONGODB_URI=mongodb://localhost:27017
    export CLEAN_HANDS_ISSUER_ADDRESS=3953516660401541564649985379958697237340496801951929947163239598560489169274
    # The following variables MUST be changed
    export ATTESTOR_PRIVATE_KEY=123 
    export OP_RPC_URL=abc
    export SUI_PRIVATE_KEY=123
    export SUI_PRIVATE_KEY_SCHEME=ed25519

From directory /human, execute

    cargo run --bin observer

### Diagrams

How the Observer fits into the Clean Hands flow in Zeronym.

#### Full flow
```mermaid
sequenceDiagram
    participant U as User
    participant Z as Zeronym
    participant S as Sanctions lists
    participant O as Observer
    participant A as Attestation contract
    participant D as Decryptor
    participant M as Human

    Note over U, S: 1. Credential issuance
    U->>Z: Verify ID
    Z->>S: Query
    S->>Z: Return list of hits
    Z->>Z: Make sure there are no hits
    Z->>U: Send signed credentials
    Note over U, O: 2. Proof generation
    U->>U: Generate proof of encryption
    U->>U: Sign conditions contract
    Note over U, A: 3. Attestation issuance
    U->>O: Send ZKP & signature of smart contract
    O->>O: Verify ZKP
    O->>O: Store ciphertext output by ZKP
    O->>A: Issue attestation to user
    Note over U, A: (4. User does stuff on-chain...)
    Note over O, M: 5. Decryption
    D->>O: Query with user address
    O->>D: Send user's ciphertext and contract signature
    D->>M: Send C1 (from ciphertext), signature, and contract
    M->>M: Verify C1 against signature
    M->>M: Verify that contract grants the requester access
    M->>D: Return shared secret
    D->>D: Use shared secret to decrypt ciphertext
```

#### 1. Credential issuance flow
```mermaid
sequenceDiagram
    participant U as User
    participant Z as Zeronym
    participant S as Sanctions lists

    U->>Z: Verify ID
    Z->>S: Query
    S->>Z: List of hits
    Z->>Z: Make sure num hits < 1
    Z->>U: Signed credentials
```

#### 2. Proof generation
```mermaid
sequenceDiagram
    participant U as User

    U->>U: Generate proof of encryption
    U->>U: Sign conditions contract
```

#### 3. Attestation issuance
```mermaid
sequenceDiagram
    participant U as User
    participant O as Observer
    participant A as Attestation contract

    U->>O: Send ZKP & signature of smart contract
    O->>O: Verify ZKP
    O->>O: Store ciphertext output by ZKP
    O->>A: Issue attestation to user
```

#### (4. On-chain activity)

#### 5. Decryption
```mermaid
sequenceDiagram
    participant O as Observer
    participant D as Decryptor
    participant M as Human

    D->>O: Query with user address
    O->>D: Send user's ciphertext and contract signature
    D->>M: Send C1 (from ciphertext), signature, and contract
    M->>M: Verify C1 against signature
    M->>M: Verify that contract grants the requester access
    M->>D: Return shared secret
    D->>D: Use shared secret to decrypt ciphertext
```
