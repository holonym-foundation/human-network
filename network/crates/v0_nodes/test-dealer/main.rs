use ark_ed_on_bn254::Fr;
use ff::PrimeField;
use k256::Scalar;
use human_crypto::{polynomial::Polynomial, BabyJubJub, Curve, F256k1, PointTrait, ScalarTrait, Secp256k1, BABYJUBJUB};
use rand::rngs::ThreadRng;
const T: usize = 4;
const N: usize = 15;
fn main() {
    let secp_oprf_poly = Polynomial::<F256k1>::random_polynomial(T - 1);
    let secp_jwtprf_poly = Polynomial::<F256k1>::random_polynomial(T - 1);
    let babyjub_decrypt_poly = Polynomial::<BABYJUBJUB>::random_polynomial(T - 1);
    for idx in (1..N + 1) {
        let secp_oprf_keyshare: Scalar = secp_oprf_poly.eval(&F256k1::from_u128(idx as u128)).into();
        let secp_jwtprf_keyshare: Scalar = secp_jwtprf_poly.eval(&F256k1::from_u128(idx as u128)).into();
        let babyjub_decrypt_keyshare: Fr = babyjub_decrypt_poly.eval(&BABYJUBJUB::from_u128(idx as u128)).into();
        println!("SECP256K1_OPRF_KEYSHARE_PROVER{}={}", idx, hex::encode(secp_oprf_keyshare.to_biguint_vartime().to_bytes_be()));
        println!("SECP256K1_JWTPRF_KEYSHARE_PROVER{}={}", idx, hex::encode(secp_jwtprf_keyshare.to_biguint_vartime().to_bytes_be()));
        println!("BABYJUBJUB_DECRYPT_KEYSHARE_PROVER{}={}", idx, hex::encode(babyjub_decrypt_keyshare.to_biguint_vartime().to_bytes_be()));
    }
}
