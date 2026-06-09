//! Test suite for the Web and headless browsers.

#![cfg(target_arch = "wasm32")]
extern crate wasm_bindgen_test;
// use humanwasm::{generate_ephemeral_key_and_sign_conditions_with_it, prf_step1};
use wasm_bindgen_test::*;
wasm_bindgen_test_configure!(run_in_browser);
#[wasm_bindgen_test]
fn test_signing_w_new_rand_privkey() {
    // generate_ephemeral_key_and_sign_conditions_with_it("0x3a0d4A524Aa53A29959Aaef1Cff899F35Cc7F766".to_string());
}
// #[wasm_bindgen_test]
// fn test_request() {
//     prf_step1("abc");
// }
