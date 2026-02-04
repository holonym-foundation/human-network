#!/bin/bash


# Function to handle build errors
function build_project {
    echo "Building project..."
    cargo build --release --package node --bin human_node -p actors --features "local_test_net" -p network --features "local_test_net"
#    cargo build --release --package registry_iface --bin registry_iface
#    cargo build --release --package cli --bin cli
    if [[ "$?" -ne 0 ]]; then
        echo "Error: Cargo build failed."
        exit 1
    fi
}

get_bootstrap_peer_id() {
    node ../scripts/test/get-peer-id.js 0 http://127.0.0.1:8540
}

get_relay_peer_id() {
    node ../scripts/test/get-peer-id.js 1 http://127.0.0.1:8540
}

bootstrap_multiaddr="/ip4/127.0.0.1/udp/8080/quic-v1"
relay_multiaddr="/ip4/127.0.0.1/udp/8081/quic-v1"

get_bootstrap_and_relay_args() {
    echo "--bootstrap-peer-id $(get_bootstrap_peer_id) --bootstrap-multiaddr $bootstrap_multiaddr --relay-peer-id $(get_relay_peer_id) --relay-multiaddr $relay_multiaddr"
}

rsa_seeds=(
    "d9d0814e23e839bcadb01dfb49c8e9e0cdce6ba82584d284c2a1518f97cc447c" \
    "46566c7aee67c77d3608e5fc5cafbd716c027c8c7da7bf409fed0ff6af48959e" \
    "dcf210c787b8ff4b3006213df20dd0981e3bc0711197e05cb62cbb4290a8ab0c" \
    "ec9e43ea19bb3cb0e6bd1b90ae26818311e6509efd2c384b21a7a25ddba8c7fb" \
    "75bfc6043cc6896f0f82bff460abd7cb540c2ea32bb581fbc7f01c4b65a6e81e" \
    "447370c3e98a1636cc5a2ffc08fb727a2c85e72228ed515b607ad76123cf0160" \
    "40bd25e1bb931cac91a585b176deab84fe7422535749bd13b9b0616da0321352" \
    "e79a764e2549820554d201d24c38f7228e03363d77b634a1097d11ca441dbf60" \
    "e65d8194a7b73236097130356cfe3f4d51e95578eed4300ff9be4cf0cccc36f8" \
    "492b3ff6b32fc3fe3fd19dfbfe50356a12ec804b1914362e6d313dae3e8bc081" \
    "550427bbc52ef207950f49624c92ea64b6af2f369a390c8da1453c97dc13d0a0" \
    "94631675ea762a5d89ef8827c7984cedb6bc5e6763679891e71cf66d02141fdf" \
    "eb019c29b7d75148e3740c4132665bc904b32251b2eae1e88628ce5604558a01" \
    "7b917c2886e3e75215f33a978e01bbffadb29ca878ae65dd1e7b63266447e281" \
    "47b2ab505b041354d49ba74ea89fbeb4ad1efd1cb7d713d8d5c7d33d68980433" \
    "f8b1cacf86133a2107ddc1614591db0988927e6b0078a62cea2e84da11442b55" \
    "404c901fb7e932200774f0ebb32798b6f6962dd42120d4b2d968f7149a788959" \
    "b1f0b61c5e4ec3b2b920c7a5e4a8c0a347f42714054b404d2a506945497de967" \
    "5a6c448104995c3201079b9dbc09d39b7533ef3f9f04f784ac7aea04f93a6df2" \
    "8f82c7b262774d961403e7f1410e74a8a3ab7d71828c847a612d7bf9e6ab5a69" \
    "87f57bdcd4c3be80fe7a43780a35d563d688dd47389b831d093a85888cbfc2ce" \
    "ab0d983e3be972f81806e34d111385beae4162dd0ac478df11a17e7b9844d094" \
)

secp256k1_seeds=( \
    "2130bd591c08948c38a391a92621c0f23268a5683e55c296956158eb981614fe" \
    "0a4f66e0189bf85d5fd2f1f3b67a736a0d5ae577d6b24aa9d62021c3062a8d18" \
    "26b6bb53f22c62baa4aa99b275ea17010dedfe124eb3c0851e3af96ba4faf2e8" \
    "32ee679683f45e1c96aab685bd3c9029e156f80f221b05e60493fb048052ee6c" \
    "7f9ca45079877f1c23745fb2343902ee94e00f8d8f02c515aba7a1433d8f1125" \
    "17ddce25cfcfc9d1b934ab7e2c2667147764cfd3dfa008898893fe445d2ce358" \
    "966a40c2118a02e2ab1b3cdc24893a30fe0938a1b91d2fb7831f4a3d3a9cea22" \
    "11b51275234415409a191f45272b405352919fbd252b6d64a11dbd5ae29d2372" \
    "a0244a218da622f6e94158d908062883626401d0f275765fe9b6f2bb889b173a" \
    "7249c5b72ef8ad7e7d5d943698a9f73c30f1a4625d7380ffaa0339efb0956eb6" \
    "7b88fe6edb3bd6f28877bf923faf243b2dc0fbddfecb856fa4f68c0809fbbf9b" \
    "c04fbd53c776c0c3f78449436f4bd3a373b38162e086a0283d826bd8df2e8c57" \
    "9c7fad54205512c915e726d78a01879529f2ac9b2e86d35b5916ab128d2826c4" \
    "5f7753d608fd3e4c7fec8216ecc6f178a07e6bac7d4076ec2393fdaec857d8e4" \
    "9bba70acb11ed82ca52517956bdfc5486cc85e9dfbafbab618a70928bbb2cae0" \
    "dbebe0c0c406bc5f29464de1257cc744eeb57b41f12be36ea62b2e113af9ddaa" \
    "9dbe94ef5e0d328f2ea2cc5d0e82ee5c2dc2af438e90ae978e4124c0c014d194" \
    "2cffc6499a1a320eed36627ca8be3dab256a362b56e7ae02ff5abba4d7b5189b" \
    "7fc89084c783882a9054f22a1fe04035c77ca14d7e4dfea6c28b3b5f757229bb" \
    "c3e12259a88eb7c2ca121b5040a6a44d4efafcc66e6b726580a96a9adf75e2a0" \
    "43c00ba840e2a3fa15f59fe5940779fbf15239d6f00147732b51014dca08d189" \
    "2e57cde74bc9b2d207baa42463242cdb15f0fb3efb7b88ccad05ee46ce6884dd" \
)

# Check command-line arguments
case "$1" in
    build)
        build_project
        ;;
    
    run)
        case "$2" in
            -bootstrap)
                echo "Starting bootstrap node...";
                export RSA_SEED=d9d0814e23e839bcadb01dfb49c8e9e0cdce6ba82584d284c2a1518f97cc447c
                export SECP256K1_SEED=2130bd591c08948c38a391a92621c0f23268a5683e55c296956158eb981614fe 
                export NODE_TYPE=Bootstrap
                export NODE_MULTIADDR=/ip4/127.0.0.1/udp/8080/quic-v1
                RUSTLOG=INFO target/release/human_node $(get_bootstrap_and_relay_args) --port 8080 --redis redis://127.0.0.1:6379/0 | bunyan &
                sleep 1
                echo "Waiting for bootstrap node to startup..."
                wait
                ;;
            
            -relay)
                echo "Starting relay node..."
                export RSA_SEED=46566c7aee67c77d3608e5fc5cafbd716c027c8c7da7bf409fed0ff6af48959e
                export SECP256K1_SEED=0a4f66e0189bf85d5fd2f1f3b67a736a0d5ae577d6b24aa9d62021c3062a8d18
                export NODE_TYPE=Relay
                export NODE_MULTIADDR=/ip4/127.0.0.1/udp/8081/quic-v1
                RUSTLOG=INFO target/release/human_node $(get_bootstrap_and_relay_args) --port 8081 --redis redis://127.0.0.1:6379/1 | bunyan &
                sleep 2
                echo "Waiting for relay node to startup..."
                wait
                ;;
            
            -prover)
                if [[ "$3" == "-idx" && "$4" =~ ^-?[0-9]+$ ]]; then
                    idx=$4
                    echo "idx" $idx
                    port=$((8080 + $idx))
                    redis_port=6379
                    echo "Starting prover node with index $idx... Port "$port;
                    export RSA_SEED=${rsa_seeds[$idx]}
                    export SECP256K1_SEED=${secp256k1_seeds[$idx]}
                    export NODE_TYPE=Prover
                    export NODE_MULTIADDR="/ip4/127.0.0.1/udp/$port/quic-v1"
                    echo "Seed" $SECP256K1_SEED
                    echo "RSA Seed "$RSA_SEED
                    RUSTLOG=INFO target/release/human_node $(get_bootstrap_and_relay_args) --port $port --redis redis://127.0.0.1:$redis_port/$idx | bunyan &
                    wait
                else
                    echo "Error: Invalid prover index."
                    exit 1
                fi
                ;;
            
            *)
                echo "Usage: $0 build"
                echo "       $0 generate <number_of_keys>"
                echo "       $0 run -bootstrap"
                echo "       $0 run -relay"
                echo "       $0 run -prover <number_of_nodes>"
                echo "       $0 run -prover -idx <index>"
                exit 1
                ;;
        esac
        ;;
    
    *)
        echo "Usage: $0 build"
        echo "       $0 generate <number_of_keys>"
        echo "       $0 run -bootstrap"
        echo "       $0 run -relay"
        echo "       $0 run -prover <number_of_nodes>"
        echo "       $0 run -prover -idx <index>"
        exit 1
        ;;
esac
