WORKDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

echo 'Registering Bootstrap node...'
RSA_SEED=d9d0814e23e839bcadb01dfb49c8e9e0cdce6ba82584d284c2a1518f97cc447c \
SECP256K1_SEED=2130bd591c08948c38a391a92621c0f23268a5683e55c296956158eb981614fe \
NODE_TYPE=Bootstrap \
NODE_MULTIADDR=/ip4/127.0.0.1/udp/8080/quic-v1 \
$WORKDIR/../../network/target/release/registry_iface register \
    --rpc-url http://127.0.0.1:8540 \
    --private-key 0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80 \
    --multiaddr /ip4/127.0.0.1/udp/8080/quic-v1 \
    --rpcaddr http://127.0.0.1:8080 \
    --test true;

echo 'Registering Relay node...';
RSA_SEED=46566c7aee67c77d3608e5fc5cafbd716c027c8c7da7bf409fed0ff6af48959e \
SECP256K1_SEED=0a4f66e0189bf85d5fd2f1f3b67a736a0d5ae577d6b24aa9d62021c3062a8d18 \
NODE_TYPE=Relay \
NODE_MULTIADDR=/ip4/127.0.0.102/udp/8081/quic-v1 \
$WORKDIR/../../network/target/release/registry_iface register \
    --rpc-url http://127.0.0.1:8540 \
    --private-key 0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d \
    --multiaddr /ip4/127.0.0.1/udp/8081/quic-v1 \
    --rpcaddr http://127.0.0.1:8081 \
    --test true;

echo 'Registering Prover 1...';
RSA_SEED=dcf210c787b8ff4b3006213df20dd0981e3bc0711197e05cb62cbb4290a8ab0c \
SECP256K1_SEED=26b6bb53f22c62baa4aa99b275ea17010dedfe124eb3c0851e3af96ba4faf2e8 \
NODE_TYPE=Prover \
NODE_MULTIADDR=/ip4/127.0.0.103/udp/8082/quic-v1 \
$WORKDIR/../../network/target/release/registry_iface register \
    --rpc-url http://127.0.0.1:8540 \
    --private-key 0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a \
    --multiaddr /ip4/127.0.0.1/udp/8082/quic-v1 \
    --rpcaddr http://127.0.0.1:8082 \
    --test true;

echo 'Registering Prover 2...';
RSA_SEED=ec9e43ea19bb3cb0e6bd1b90ae26818311e6509efd2c384b21a7a25ddba8c7fb \
SECP256K1_SEED=32ee679683f45e1c96aab685bd3c9029e156f80f221b05e60493fb048052ee6c \
NODE_TYPE=Prover \
NODE_MULTIADDR=/ip4/127.0.0.104/udp/8083/quic-v1 \
$WORKDIR/../../network/target/release/registry_iface register \
    --rpc-url http://127.0.0.1:8540 \
    --private-key 0x7c852118294e51e653712a81e05800f419141751be58f605c371e15141b007a6 \
    --multiaddr /ip4/127.0.0.1/udp/8083/quic-v1 \
    --rpcaddr http://127.0.0.1:8083 \
    --test true;

echo 'Registering Prover 3...';
RSA_SEED=75bfc6043cc6896f0f82bff460abd7cb540c2ea32bb581fbc7f01c4b65a6e81e \
SECP256K1_SEED=7f9ca45079877f1c23745fb2343902ee94e00f8d8f02c515aba7a1433d8f1125 \
NODE_TYPE=Prover \
NODE_MULTIADDR=/ip4/127.0.0.105/udp/8084/quic-v1 \
$WORKDIR/../../network/target/release/registry_iface register \
    --rpc-url http://127.0.0.1:8540 \
    --private-key 0x47e179ec197488593b187f80a00eb0da91f1b9d0b13f8733639f19c30a34926a \
    --multiaddr /ip4/127.0.0.1/udp/8084/quic-v1 \
    --rpcaddr http://127.0.0.1:8084 \
    --test true;

echo 'Registering Prover 4...';
RSA_SEED=447370c3e98a1636cc5a2ffc08fb727a2c85e72228ed515b607ad76123cf0160 \
SECP256K1_SEED=17ddce25cfcfc9d1b934ab7e2c2667147764cfd3dfa008898893fe445d2ce358 \
NODE_TYPE=Prover \
NODE_MULTIADDR=/ip4/127.0.0.106/udp/8085/quic-v1 \
$WORKDIR/../../network/target/release/registry_iface register \
    --rpc-url http://127.0.0.1:8540 \
    --private-key 0x8b3a350cf5c34c9194ca85829a2df0ec3153be0318b5e2d3348e872092edffba \
    --multiaddr /ip4/127.0.0.1/udp/8085/quic-v1 \
    --rpcaddr http://127.0.0.1:8085 \
    --test true;

echo 'Registering Prover 5...';
RSA_SEED=40bd25e1bb931cac91a585b176deab84fe7422535749bd13b9b0616da0321352 \
SECP256K1_SEED=966a40c2118a02e2ab1b3cdc24893a30fe0938a1b91d2fb7831f4a3d3a9cea22 \
NODE_TYPE=Prover \
NODE_MULTIADDR=/ip4/127.0.0.107/udp/8086/quic-v1 \
$WORKDIR/../../network/target/release/registry_iface register \
    --rpc-url http://127.0.0.1:8540 \
    --private-key 0x92db14e403b83dfe3df233f83dfa3a0d7096f21ca9b0d6d6b8d88b2b4ec1564e \
    --multiaddr /ip4/127.0.0.1/udp/8086/quic-v1 \
    --rpcaddr http://127.0.0.1:8086 \
    --test true;

echo 'Registering Prover 6...';
RSA_SEED=e79a764e2549820554d201d24c38f7228e03363d77b634a1097d11ca441dbf60 \
SECP256K1_SEED=11b51275234415409a191f45272b405352919fbd252b6d64a11dbd5ae29d2372 \
NODE_TYPE=Prover \
NODE_MULTIADDR=/ip4/127.0.0.108/udp/8087/quic-v1 \
$WORKDIR/../../network/target/release/registry_iface register \
    --rpc-url http://127.0.0.1:8540 \
    --private-key 0x4bbbf85ce3377467afe5d46f804f221813b2bb87f24d81f60f1fcdbf7cbf4356 \
    --multiaddr /ip4/127.0.0.1/udp/8087/quic-v1 \
    --rpcaddr http://127.0.0.1:8087 \
    --test true;

echo 'Registering Prover 7...';
RSA_SEED=e65d8194a7b73236097130356cfe3f4d51e95578eed4300ff9be4cf0cccc36f8 \
SECP256K1_SEED=a0244a218da622f6e94158d908062883626401d0f275765fe9b6f2bb889b173a \
NODE_TYPE=Prover \
NODE_MULTIADDR=/ip4/127.0.0.108/udp/8088/quic-v1 \
$WORKDIR/../../network/target/release/registry_iface register \
    --rpc-url http://127.0.0.1:8540 \
    --private-key 0xdbda1821b80551c9d65939329250298aa3472ba22feea921c0cf5d620ea67b97 \
    --multiaddr /ip4/127.0.0.1/udp/8088/quic-v1 \
    --rpcaddr http://127.0.0.1:8088 \
    --test true;

    echo 'Registering Prover 8...';
RSA_SEED=492b3ff6b32fc3fe3fd19dfbfe50356a12ec804b1914362e6d313dae3e8bc081 \
SECP256K1_SEED=7249c5b72ef8ad7e7d5d943698a9f73c30f1a4625d7380ffaa0339efb0956eb6 \
NODE_TYPE=Prover \
NODE_MULTIADDR=/ip4/127.0.0.108/udp/8089/quic-v1 \
$WORKDIR/../../network/target/release/registry_iface register \
    --rpc-url http://127.0.0.1:8540 \
    --private-key 0x2a871d0798f97d79848a013d4936a73bf4cc922c825d33c1cf7073dff6d409c6 \
    --multiaddr /ip4/127.0.0.1/udp/8089/quic-v1 \
    --rpcaddr http://127.0.0.1:8089 \
    --test true;

    echo 'Registering Prover 9...';
RSA_SEED=550427bbc52ef207950f49624c92ea64b6af2f369a390c8da1453c97dc13d0a0 \
SECP256K1_SEED=7b88fe6edb3bd6f28877bf923faf243b2dc0fbddfecb856fa4f68c0809fbbf9b \
NODE_TYPE=Prover \
NODE_MULTIADDR=/ip4/127.0.0.108/udp/8090/quic-v1 \
$WORKDIR/../../network/target/release/registry_iface register \
    --rpc-url http://127.0.0.1:8540 \
    --private-key 0xf214f2b2cd398c806f84e317254e0f0b801d0643303237d97a22a48e01628897 \
    --multiaddr /ip4/127.0.0.1/udp/8090/quic-v1 \
    --rpcaddr http://127.0.0.1:8090 \
    --test true;

    echo 'Registering Prover 10...';
RSA_SEED=94631675ea762a5d89ef8827c7984cedb6bc5e6763679891e71cf66d02141fdf \
SECP256K1_SEED=c04fbd53c776c0c3f78449436f4bd3a373b38162e086a0283d826bd8df2e8c57 \
NODE_TYPE=Prover \
NODE_MULTIADDR=/ip4/127.0.0.108/udp/8091/quic-v1 \
$WORKDIR/../../network/target/release/registry_iface register \
    --rpc-url http://127.0.0.1:8540 \
    --private-key 0x701b615bbdfb9de65240bc28bd21bbc0d996645a3dd57e7b12bc2bdf6f192c82 \
    --multiaddr /ip4/127.0.0.1/udp/8091/quic-v1 \
    --rpcaddr http://127.0.0.1:8091 \
    --test true;

    echo 'Registering Prover 11...';
RSA_SEED=eb019c29b7d75148e3740c4132665bc904b32251b2eae1e88628ce5604558a01 \
SECP256K1_SEED=9c7fad54205512c915e726d78a01879529f2ac9b2e86d35b5916ab128d2826c4 \
NODE_TYPE=Prover \
NODE_MULTIADDR=/ip4/127.0.0.108/udp/8092/quic-v1 \
$WORKDIR/../../network/target/release/registry_iface register \
    --rpc-url http://127.0.0.1:8540 \
    --private-key 0xa267530f49f8280200edf313ee7af6b827f2a8bce2897751d06a843f644967b1 \
    --multiaddr /ip4/127.0.0.1/udp/8092/quic-v1 \
    --rpcaddr http://127.0.0.1:8092 \
    --test true;

    echo 'Registering Prover 12...';
RSA_SEED=7b917c2886e3e75215f33a978e01bbffadb29ca878ae65dd1e7b63266447e281 \
SECP256K1_SEED=5f7753d608fd3e4c7fec8216ecc6f178a07e6bac7d4076ec2393fdaec857d8e4 \
NODE_TYPE=Prover \
NODE_MULTIADDR=/ip4/127.0.0.108/udp/8093/quic-v1 \
$WORKDIR/../../network/target/release/registry_iface register \
    --rpc-url http://127.0.0.1:8540 \
    --private-key 0x47c99abed3324a2707c28affff1267e45918ec8c3f20b8aa892e8b065d2942dd \
    --multiaddr /ip4/127.0.0.1/udp/8093/quic-v1 \
    --rpcaddr http://127.0.0.1:8093 \
    --test true;

    echo 'Registering Prover 13...';
RSA_SEED=47b2ab505b041354d49ba74ea89fbeb4ad1efd1cb7d713d8d5c7d33d68980433 \
SECP256K1_SEED=9bba70acb11ed82ca52517956bdfc5486cc85e9dfbafbab618a70928bbb2cae0 \
NODE_TYPE=Prover \
NODE_MULTIADDR=/ip4/127.0.0.108/udp/8094/quic-v1 \
$WORKDIR/../../network/target/release/registry_iface register \
    --rpc-url http://127.0.0.1:8540 \
    --private-key 0xc526ee95bf44d8fc405a158bb884d9d1238d99f0612e9f33d006bb0789009aaa \
    --multiaddr /ip4/127.0.0.1/udp/8094/quic-v1 \
    --rpcaddr http://127.0.0.1:8094 \
    --test true;

    echo 'Registering Prover 14...';
RSA_SEED=f8b1cacf86133a2107ddc1614591db0988927e6b0078a62cea2e84da11442b55 \
SECP256K1_SEED=dbebe0c0c406bc5f29464de1257cc744eeb57b41f12be36ea62b2e113af9ddaa \
NODE_TYPE=Prover \
NODE_MULTIADDR=/ip4/127.0.0.108/udp/8095/quic-v1 \
$WORKDIR/../../network/target/release/registry_iface register \
    --rpc-url http://127.0.0.1:8540 \
    --private-key 0x8166f546bab6da521a8369cab06c5d2b9e46670292d85c875ee9ec20e84ffb61 \
    --multiaddr /ip4/127.0.0.1/udp/8095/quic-v1 \
    --rpcaddr http://127.0.0.1:8095 \
    --test true;

    echo 'Registering Prover 15...';
RSA_SEED=404c901fb7e932200774f0ebb32798b6f6962dd42120d4b2d968f7149a788959 \
SECP256K1_SEED=9dbe94ef5e0d328f2ea2cc5d0e82ee5c2dc2af438e90ae978e4124c0c014d194 \
NODE_TYPE=Prover \
NODE_MULTIADDR=/ip4/127.0.0.108/udp/8096/quic-v1 \
$WORKDIR/../../network/target/release/registry_iface register \
    --rpc-url http://127.0.0.1:8540 \
    --private-key 0xea6c44ac03bff858b476bba40716402b03e41b8e97e276d1baec7c37d42484a0 \
    --multiaddr /ip4/127.0.0.1/udp/8096/quic-v1 \
    --rpcaddr http://127.0.0.1:8096 \
    --test true;

    echo 'Registering Prover 16...';
RSA_SEED=b1f0b61c5e4ec3b2b920c7a5e4a8c0a347f42714054b404d2a506945497de967 \
SECP256K1_SEED=2cffc6499a1a320eed36627ca8be3dab256a362b56e7ae02ff5abba4d7b5189b \
NODE_TYPE=Prover \
NODE_MULTIADDR=/ip4/127.0.0.108/udp/8097/quic-v1 \
$WORKDIR/../../network/target/release/registry_iface register \
    --rpc-url http://127.0.0.1:8540 \
    --private-key 0x689af8efa8c651a91ad287602527f3af2fe9f6501a7ac4b061667b5a93e037fd \
    --multiaddr /ip4/127.0.0.1/udp/8097/quic-v1 \
    --rpcaddr http://127.0.0.1:8097 \
    --test true;

echo 'Registering Prover 17...';
RSA_SEED=5a6c448104995c3201079b9dbc09d39b7533ef3f9f04f784ac7aea04f93a6df2 \
SECP256K1_SEED=7fc89084c783882a9054f22a1fe04035c77ca14d7e4dfea6c28b3b5f757229bb \
NODE_TYPE=Prover \
NODE_MULTIADDR=/ip4/127.0.0.108/udp/8098/quic-v1 \
$WORKDIR/../../network/target/release/registry_iface register \
    --rpc-url http://127.0.0.1:8540 \
    --private-key 0xde9be858da4a475276426320d5e9262ecfc3ba460bfac56360bfa6c4c28b4ee0 \
    --multiaddr /ip4/127.0.0.1/udp/8098/quic-v1 \
    --rpcaddr http://127.0.0.1:8098 \
    --test true;

    echo 'Registering Prover 18...';
RSA_SEED=8f82c7b262774d961403e7f1410e74a8a3ab7d71828c847a612d7bf9e6ab5a69 \
SECP256K1_SEED=c3e12259a88eb7c2ca121b5040a6a44d4efafcc66e6b726580a96a9adf75e2a0 \
NODE_TYPE=Prover \
NODE_MULTIADDR=/ip4/127.0.0.108/udp/8099/quic-v1 \
$WORKDIR/../../network/target/release/registry_iface register \
    --rpc-url http://127.0.0.1:8540 \
    --private-key 0xdf57089febbacf7ba0bc227dafbffa9fc08a93fdc68e1e42411a14efcf23656e \
    --multiaddr /ip4/127.0.0.1/udp/8099/quic-v1 \
    --rpcaddr http://127.0.0.1:8099 \
    --test true;

    echo 'Registering Prover 19...';
RSA_SEED=87f57bdcd4c3be80fe7a43780a35d563d688dd47389b831d093a85888cbfc2ce \
SECP256K1_SEED=43c00ba840e2a3fa15f59fe5940779fbf15239d6f00147732b51014dca08d189 \
NODE_TYPE=Prover \
NODE_MULTIADDR=/ip4/127.0.0.108/udp/8100/quic-v1 \
$WORKDIR/../../network/target/release/registry_iface register \
    --rpc-url http://127.0.0.1:8540 \
    --private-key 0xeaa861a9a01391ed3d587d8a5a84ca56ee277629a8b02c22093a419bf240e65d \
    --multiaddr /ip4/127.0.0.1/udp/8100/quic-v1 \
    --rpcaddr http://127.0.0.1:8100 \
    --test true;

    echo 'Registering Prover 20...';
RSA_SEED=ab0d983e3be972f81806e34d111385beae4162dd0ac478df11a17e7b9844d094 \
SECP256K1_SEED=2e57cde74bc9b2d207baa42463242cdb15f0fb3efb7b88ccad05ee46ce6884dd \
NODE_TYPE=Prover \
NODE_MULTIADDR=/ip4/127.0.0.108/udp/8101/quic-v1 \
$WORKDIR/../../network/target/release/registry_iface register \
    --rpc-url http://127.0.0.1:8540 \
    --private-key 0xc511b2aa70776d4ff1d376e8537903dae36896132c90b91d52c1dfbae267cd8b \
    --multiaddr /ip4/127.0.0.1/udp/8101/quic-v1 \
    --rpcaddr http://127.0.0.1:8101 \
    --test true;