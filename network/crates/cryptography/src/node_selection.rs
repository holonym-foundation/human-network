use itertools::Itertools;
use lazy_static::lazy_static;
use num_bigint::BigUint;
use num_traits::ToPrimitive;
use rand::{Rng, RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use tracing::{debug, error, info, warn};

lazy_static! {
    pub static ref MIN_NODES: AtomicUsize = AtomicUsize::new(3);
    pub static ref EPSILON: AtomicU32 = AtomicU32::new(1);
    pub static ref THRESHOLD: AtomicU64 = AtomicU64::new(60f64.to_bits());
}

pub fn get_min_nodes() -> usize { MIN_NODES.load(Ordering::Relaxed) }

pub fn set_min_nodes(val: usize) { MIN_NODES.store(val, Ordering::Relaxed); }

pub fn get_epsilon() -> u32 { EPSILON.load(Ordering::Relaxed) }

pub fn set_epsilon(val: u32) { EPSILON.store(val, Ordering::Relaxed); }

pub fn get_threshold() -> f64 { f64::from_bits(THRESHOLD.load(Ordering::Relaxed)) }

pub fn set_threshold(val: f64) { THRESHOLD.store(val.to_bits(), Ordering::Relaxed); }

/// Allows a deterministic, uniform selection of t + epsilon nodes.
/// This way, each request should be mapped to a particular set of nodes.
/// Since it is uniform, this enables us to assume that all nodes should are
/// requested from equally. This simplifies accounting, so all nodes can be paid
/// equally assuming this mechanism works and they respond to all their requests.
/// n is the number of nodes, t is the threshold, and epsilon is the number of extra nodes to query
pub fn calc_nodes_to_send_to(epoch_num: u32, request_num: u128, t_plus_epsilon: u32, node_indices: Vec<usize>) -> Vec<u32> {
    let mut node_indices = node_indices.clone();
    let concatted: [u8; 32] = [(epoch_num as u128).to_be_bytes(), request_num.to_be_bytes()].concat().try_into().unwrap();
    info!(
        "Generating a 32-byte seed by concatenating the big-endian byte representations of epoch_num ({} as u128) and request_num ({}). \
        The resulting byte array is then converted into a fixed-size [u8; 32] array. Seed: {:02x?}  \
        node_indices :{:?}",
        epoch_num, request_num, concatted, node_indices
    );
    let mut rng = ChaCha8Rng::from_seed(concatted);
    let mut nodes_chosen = Vec::with_capacity(t_plus_epsilon as usize);
    for _i in 0..t_plus_epsilon {
        let num = rng.next_u64();
        info!("Rand Num :{} Nodes :{} ,Node chosen :{}", num, node_indices.len(), num % node_indices.len() as u64);
        let node_chosen_idx = num % node_indices.len() as u64;
        //    let node_chosen_idx = rng.next_u64() % (node_indices.len() as u64); // as long as there are not over 2^32 nodes, we don't have to worry about uniformity after modular reduction :) :)
        let node_chosen = node_indices.remove(node_chosen_idx as usize) as u32;
        // println!("Node chosen: {}", node_chosen);
        nodes_chosen.push(node_chosen);
    }
    nodes_chosen
}

fn biguint_to_f64(value: &BigUint) -> f64 {
    // Convert to bytes in big-endian order
    let bytes = value.to_bytes_be();

    // Accumulate the floating-point value
    let mut result = 0.0;
    for (i, byte) in bytes.iter().enumerate() {
        let shift = (bytes.len() - 1 - i) * 8;
        result += (*byte as f64) * 2f64.powi(shift as i32);
    }
    result
}

/// A faster function to just calculate whether it should be sent to a particular node
pub fn should_be_sent_to_node(epoch_num: u32, request_num: u128, t_plus_epsilon: u32, node: u32, node_indexes: &[usize]) -> bool {
    let seed: [u8; 32] = [(epoch_num as u128).to_be_bytes(), request_num.to_be_bytes()].concat().try_into().unwrap();
    info!(
        "Generating a 32-byte seed by concatenating the big-endian byte representations of epoch_num ({} as u128) and request_num ({}). \
        The resulting byte array is then converted into a fixed-size [u8; 32] array. Seed: {:02x?}",
        epoch_num, request_num, seed
    );
    let mut rng = ChaCha8Rng::from_seed(seed);
    let mut node_indexes = node_indexes.to_vec();
    node_indexes.sort();

    for _ in 0..t_plus_epsilon {
        let num = rng.next_u64();
        info!("Rand Num :{} Nodes :{} ,Node chosen :{}", num, node_indexes.len(), num % node_indexes.len() as u64);
        let node_chosen_idx = num % node_indexes.len() as u64;
        let node_chosen_idx = node_indexes.remove(node_chosen_idx as usize);
        if node_chosen_idx == node as usize {
            return true;
        }
    }

    false
}
// ----------------------------------------------------------------------------------------------------------------------------------------------------
pub trait HasWeight {
    fn weight(&self) -> BigUint;
}
/// Chooses a node based on weight, such as stake
/// This could be more efficient but it's not a bottleneck. Alias method has O(1) time after O(n) preprocessing time if we want to optimize this by rewriting it
/// This method is easiest visualised:
/// Imagine all the weights are lines as long as their weights, merged into one line:
/// [ .................., ...., ..........., .........] (without the spaces and commas)
/// Then we choose a random number between 0 and the total weight, and find the first node that has a cumulative weight greater than or equal to the random number is chosen
/// [ .................., ...., ...*......., .........]
/// As you can see, the likelihood of a node being chosen is proportional to its weight
///

fn weighted_sample<T: HasWeight, R: Rng>(nodes: &[T], rng: &mut R) -> Option<usize> {
    // 1. Get the cumulative sum
    let cum_sum = nodes
        .iter()
        .scan(BigUint::ZERO, |acc, node| {
            tracing::info!("Accumulating weight: {}", node.weight());
            // let weight = {
            //     if value.is_zero() {
            //        value=BigUint::from(10u128);
            //     }
            //     let bit_length = value.bits();
            //     let lower_power_of_2 = BigUint::one() << (bit_length - 1);
            //     let fractional = biguint_to_f64(&value) / biguint_to_f64(&lower_power_of_2);
            //     info!("Adjusted Node Weight  :{:?}", bit_length as f64 - 1.0 + fractional.log2());
            //     BigUint::from_f64(bit_length as f64 - 1.0 + fractional.log2())
            // };

            *acc += node.weight();
            // *acc += weight.unwrap();
            Some(acc.clone())
        })
        .collect_vec();
    // 2. Get total weight
    let total_weight = cum_sum.last()?.clone();
    // If the total weight is 0, we can't choose anything
    if total_weight == BigUint::ZERO {
        info!("Returning None since total weight is : {}", total_weight);
        return None;
    }

    // 3. Sample random number in [0, total_weight)
    let r = rng.gen_range(BigUint::ZERO..total_weight.clone());
    // 4. Find the first node with cum_sum >= r
    let mut idx = None;
    for (i, _) in cum_sum.iter().enumerate().take(nodes.len()) {
        if cum_sum[i] >= r {
            idx = Some(i);
            break;
        }
    }
    idx
}

/// Weighted sampling with square-root adjusted weights
pub fn weighted_sample_with_sqrt<T: HasWeight, R: Rng>(
    nodes: &[T],
    rng: &mut R,
) -> Option<usize> {
    // Convert all weights using sqrt
    let weights: Vec<f64> = nodes
        .iter()
        .map(|node| {
            let w = node.weight().to_f64().unwrap_or(0.0);
            w.sqrt() // √weight
        })
        .collect();
    // Total adjusted weight
    let total_weight: f64 = weights.iter().sum();
    if total_weight == 0.0 {
        return None;
    }
    // Sample random float in [0, total_weight)
    let r: f64 = rng.gen_range(0.0..total_weight);
    // Find the corresponding index
    let mut acc = 0.0;
    for (i, w) in weights.iter().enumerate() {
        acc += w;
        if acc >= r {
            return Some(i);
        }
    }
    // Should not happen, fallback
    Some(weights.len() - 1)
}

/// Weighted sampling with logarithm-adjusted weights
pub fn weighted_sample_with_log<T: HasWeight, R: Rng>(
    nodes: &[T],
    rng: &mut R,
) -> Option<usize> {
    // Convert all weights using log(weight + 1)
    let weights: Vec<f64> = nodes
        .iter()
        .map(|node| {
            let w = node.weight().to_f64().unwrap_or(0.0);
            if w <= 0.0 {
                0.0
            } else {
                w.ln_1p() // log(1 + weight)
            }
        })
        .collect();

    // Total adjusted weight
    let total_weight: f64 = weights.iter().sum();
    if total_weight == 0.0 {
        return None;
    }

    // Sample random float in [0, total_weight)
    let r: f64 = rng.gen_range(0.0..total_weight);

    // Find the corresponding index
    let mut acc = 0.0;
    for (i, w) in weights.iter().enumerate() {
        acc += w;
        if acc >= r {
            return Some(i);
        }
    }

    // Should not happen, fallback
    Some(weights.len() - 1)
}


pub fn weighted_sample_n_no_sqrt<T: HasWeight + Clone>(
    nodes: Vec<T>,
    n: usize,
    random_seed: [u8; 32],
) -> Option<Vec<T>> {
    let len = nodes.len();
    info!(
        "Starting weighted sampling without replacement: total nodes = {}, sample size = {}",
        len, n
    );
    if len == 0 || n == 0 || n > len {
        warn!("Invalid sampling request: len = {}, n = {}. Returning None.", len, n);
        return None;
    }
    debug!("Initializing ChaCha8Rng with provided seed.");
    let mut rng = ChaCha8Rng::from_seed(random_seed);
    let mut available_nodes = nodes.clone();
    let mut result = Vec::with_capacity(n);
    for i in 0..n {
        debug!("Sampling iteration {}/{}", i + 1, n);
        match weighted_sample_with_sqrt(&available_nodes, &mut rng) {
            Some(idx) => {
                info!("Selected index {} from weighted sample.", idx);
                result.push(available_nodes.remove(idx));
            }
            None => {
                error!("Weighted sampling failed at iteration {}. Aborting.", i + 1);
                return None;
            }
        }
    }
    info!("Successfully completed weighted sampling. Sampled {} nodes.", n);
    Some(result)
}


pub fn weighted_sample_n_no_classic<T: HasWeight + Clone>(
    nodes: Vec<T>,
    n: usize,
    random_seed: [u8; 32],
) -> Option<Vec<T>> {
    let len = nodes.len();
    info!(
        "Starting weighted sampling without replacement: total nodes = {}, sample size = {}",
        len, n
    );
    if len == 0 || n == 0 || n > len {
        warn!("Invalid sampling request: len = {}, n = {}. Returning None.", len, n);
        return None;
    }
    debug!("Initializing ChaCha8Rng with provided seed.");
    let mut rng = ChaCha8Rng::from_seed(random_seed);
    let mut available_nodes = nodes.clone();
    let mut result = Vec::with_capacity(n);
    for i in 0..n {
        debug!("Sampling iteration {}/{}", i + 1, n);
        match weighted_sample(&available_nodes, &mut rng) {
            Some(idx) => {
                info!("Selected index {} from weighted sample.", idx);
                result.push(available_nodes.remove(idx));
            }
            None => {
                error!("Weighted sampling failed at iteration {}. Aborting.", i + 1);
                return None;
            }
        }
    }
    info!("Successfully completed weighted sampling. Sampled {} nodes.", n);
    Some(result)
}


pub fn weighted_sample_n_no_log<T: HasWeight + Clone>(
    nodes: Vec<T>,
    n: usize,
    random_seed: [u8; 32],
) -> Option<Vec<T>> {
    let len = nodes.len();
    info!(
        "Starting weighted sampling without replacement: total nodes = {}, sample size = {}",
        len, n
    );
    if len == 0 || n == 0 || n > len {
        warn!("Invalid sampling request: len = {}, n = {}. Returning None.", len, n);
        return None;
    }
    debug!("Initializing ChaCha8Rng with provided seed.");
    let mut rng = ChaCha8Rng::from_seed(random_seed);
    let mut available_nodes = nodes.clone();
    let mut result = Vec::with_capacity(n);
    for i in 0..n {
        debug!("Sampling iteration {}/{}", i + 1, n);
        match weighted_sample_with_log(&available_nodes, &mut rng) {
            Some(idx) => {
                info!("Selected index {} from weighted sample.", idx);
                result.push(available_nodes.remove(idx));
            }
            None => {
                error!("Weighted sampling failed at iteration {}. Aborting.", i + 1);
                return None;
            }
        }
    }
    info!("Successfully completed weighted sampling. Sampled {} nodes.", n);
    Some(result)
}
/// Samples n nodes with weights without replacement. n should be <= the number of nodes with non-zero weight
pub fn weighted_sample_n_no_replace<T: HasWeight + Clone>(nodes: Vec<T>, n: usize, random_seed: [u8; 32]) -> Option<Vec<T>> {
    let len = nodes.len();
    info!("Starting weighted sampling without replacement: total nodes = {}, sample size = {}", len, n);
    if (len == 0) || (n == 0) || (n > len) {
        warn!("Invalid sampling request: len = {}, n = {}. Returning None.", len, n);
        return None;
    }

    debug!("Initializing ChaCha8Rng with provided seed.");
    let mut r = ChaCha8Rng::from_seed(random_seed);
    let mut the_nodes = nodes.clone();
    let mut result = Vec::new();

    for i in 0..n {
        debug!("Sampling iteration {}/{}", i + 1, n);
        match weighted_sample_with_sqrt(&the_nodes, &mut r) {
            Some(idx) => {
                info!("Selected index {} from weighted sample.", idx);
                result.push(the_nodes.remove(idx));
            }
            None => {
                error!("Weighted sampling failed at iteration {}. Aborting.", i + 1);
                if result.len() >= get_min_nodes() {
                    Some(result.clone());
                } else {
                    return None;
                }
            }
        }
    }

    info!("Successfully completed weighted sampling. Sampled {} nodes.", n);
    Some(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use num_traits::FromPrimitive;
    use num_traits::One;
    use rand::rngs::ThreadRng;
    use serde::{Deserialize, Serialize};
    use ethers::types::{Address, U256};




    #[derive(Debug, Clone, Serialize,Deserialize)]
    pub struct TestNode {
        pub name: &'static str,
        pub weight: U256,
    }

    impl HasWeight for TestNode {
        fn weight(&self) -> num_bigint::BigUint {
            let mut bytes = Vec::new();
            for &value in &self.weight.0 {
                bytes.extend_from_slice(&value.to_le_bytes());
            }
            BigUint::from_bytes_le(&bytes)
        }
    }
    #[test]
    fn test_calc_nodes_to_send_to() {
        let epoch_num = 123;
        let request_num = 321;
        let t_plus_epsilon = 2;
        let nodes = calc_nodes_to_send_to(epoch_num, request_num, t_plus_epsilon, vec![1, 2, 4]);
        assert_eq!(nodes.len(), t_plus_epsilon as usize);
        assert_eq!(nodes.iter().unique().count(), t_plus_epsilon as usize);
    }
    #[test]
    fn test_right_nodes_included() {
        let epoch_num = 123;
        let request_num = 321;
        let t_plus_epsilon = 2;
        let n = 3;
        let nodes = calc_nodes_to_send_to(epoch_num, request_num, t_plus_epsilon, vec![1, 2, 3]);
        println!("Nodes :{:?}", nodes);
        for node in nodes.clone() {
            assert!(should_be_sent_to_node(epoch_num, request_num, t_plus_epsilon, node, &vec![1, 2, 3]));
        }
        let not_include = (1..n + 1).filter(|node| !nodes.contains(node));
        for node in not_include {
            assert!(!should_be_sent_to_node(epoch_num, request_num, t_plus_epsilon, node, &vec![1, 2, 3]));
        }
    }
    /// For testing the stake-weighted sampling of nodes
    /// No need do derive these traits except for testing
    #[derive(Hash, Debug, Clone, Eq, PartialEq)]
    struct Node(u128);
    impl HasWeight for Node {
        fn weight(&self) -> BigUint { BigUint::from_u128(self.0).unwrap() }
    }
    #[test]
    fn test_weighted_distribution_length() {
        let nodes_0: Vec<Node> = vec![];
        let nodes_1_but_all_0 = vec![Node(0)];
        let nodes_1 = vec![Node(1)];
        let nodes_2 = vec![Node(0), Node(1)];
        let nodes_3 = vec![Node(0), Node(1), Node(2)];
        let nodes_4 = vec![Node(0), Node(1), Node(2), Node(3)];
        let mut rng: ThreadRng = ThreadRng::default();
        assert_eq!(weighted_sample_n_no_replace(nodes_0.clone(), 0, [0u8; 32]), None);
        assert_eq!(weighted_sample_n_no_replace(nodes_0.clone(), 1, [0u8; 32]), None);
        assert_eq!(weighted_sample_n_no_replace(nodes_1.clone(), 0, [0u8; 32]), None);
        assert_eq!(weighted_sample_n_no_replace(nodes_1_but_all_0, 1, [0u8; 32]), None);
        assert_eq!(weighted_sample_n_no_replace(nodes_1.clone(), 1, [0u8; 32]).unwrap().len(), 1);
        assert_eq!(weighted_sample_n_no_replace(nodes_2.clone(), 1, [0u8; 32]), None);
        assert_eq!(weighted_sample_n_no_replace(nodes_2.clone(), 2, [0u8; 32]), None);
        assert_eq!(weighted_sample_n_no_replace(nodes_2.clone(), 3, [0u8; 32]), None);
        assert_eq!(weighted_sample_n_no_replace(nodes_3.clone(), 4, [0u8; 32]), None);
        assert_eq!(weighted_sample_n_no_replace(nodes_4.clone(), 3, [0u8; 32]).unwrap().len(), 3);
        assert_eq!(weighted_sample_n_no_replace(nodes_4.clone(), 4, [0u8; 32]).unwrap().len(), 3);
    }
    #[test]
    fn test_weighted_distribution() {
        let nodes = vec![Node(0), Node(1), Node(2), Node(3), Node(4), Node(5), Node(6), Node(7), Node(8), Node(9), Node(10)];
        let n = 4;
        let results = (0..1000).map(|_| weighted_sample(&nodes, &mut ThreadRng::default()).unwrap());
        let num_5_first = results.clone().filter(|r| *r == 5).count() as f64;
        let num_1_first = results.clone().filter(|r| *r == 1).count() as f64;
        println!("5s: {}, 1s: {}", num_5_first, num_1_first);
        // Check it's at the expected probability +- 1%
        assert!(num_1_first - 18.181818 < 10.0);
        assert!(num_5_first - 90.090909 < 10.0);
    }
    #[test]
    fn test_replacement() {
        // test the items are actually removed
        let nodes = vec![Node(0), Node(1), Node(2), Node(3), Node(4), Node(5), Node(6), Node(7), Node(8), Node(9), Node(10)];
        let n = 4;
        assert!((0..500)
            .map(|_| weighted_sample_n_no_replace(nodes.clone(), n, {
                let mut rnd = [0u8; 32];
                ThreadRng::default().fill_bytes(&mut rnd);
                rnd
            })
            .unwrap())
            .all(|r| r.iter().all_unique()));
    }

    #[test]
    fn test_log2() {
        // test the items are actually removed
        let sample = vec![1u128, 2, 20, 1000, 40, 69999999];
        for nos in sample.iter() {
            let value = BigUint::from_u128(*nos).unwrap();
            let bit_length = value.bits();

            // Step 2: Compute fractional part for better precision
            // Scale the number to fit between [1, 2) in binary
            let lower_power_of_2 = BigUint::one() << (bit_length - 1);

            let fractional = biguint_to_f64(&value) / biguint_to_f64(&lower_power_of_2);

            // Combine integer and fractional parts
            println!("Value :{:?}", bit_length as f64 - 1.0 + fractional.log2());
        }
    }
    #[test]
        fn test_unbalanced_sqrt_weights() {
            let nodes = vec![
                Node(1),
                Node(10),
                Node(1000),
            ];
            let mut rng = ThreadRng::default();
            let mut counts = [0; 3];
            let mut counts_sqrt = [0; 3];
            for _ in 0..10_000 {
                if let Some(i) = weighted_sample(&nodes, &mut rng) {
                    counts[i] += 1;
                }
                if let Some(i) = weighted_sample_with_sqrt(&nodes, &mut rng) {
                    counts_sqrt[i] += 1;
                }
            }
            println!("Counts: {:?}", counts);
            assert!(counts[2] > counts[1]);
            assert!(counts[1] > counts[0]);
            println!("Counts with sqrt: {:?}", counts_sqrt);
            assert!(counts_sqrt[2] > counts_sqrt[1]);
            assert!(counts_sqrt[1] > counts_sqrt[0]);
        }
    #[test]
    fn test_balanced_weights() {
        let nodes = vec![
            Node(100),
            Node(100),
            Node(100),
        ];
        let mut rng = ThreadRng::default();
        let mut counts = [0; 3];
        for _ in 0..10_000 {
            if let Some(i) = weighted_sample_with_sqrt(&nodes, &mut rng) {
                counts[i] += 1;
            }
        }
        for count in counts.iter() {
            assert!((*count as f64) > 2500.0);
        }
    }
    #[test]
    fn test_sample_valid_fixed_seed() {
        let nodes = vec![
            Node(1),
            Node(10),
            Node(100),
            Node(50),
            Node(5),
        ];

        let seed = [42u8; 32];
        let sample = weighted_sample_n_no_sqrt(nodes.clone(), 3, seed);

        assert!(sample.is_some());
        let result = sample.unwrap();
        assert_eq!(result.len(), 3);
    }
    #[test]
    fn test_distribution_weighted_sampling_with_smaller_max_voting_power() {
        use std::collections::HashMap;
        use rand::{SeedableRng, RngCore};
        use rand_chacha::ChaCha8Rng;
        use ethers::types::U256;

        let data: Vec<(&'static str, u128)> = vec![
            // ("Moonli - EL", 1200097978771580760845),
            // ("Alchemy - S", 154026884916095712959),
            // ("Blockscape - S", 297533590790046003028),
            // ("InfraSingularity - EL", 174354829318562564437),
            // ("tokenTraxx - EL", 55191661616223093824),
            // ("Nodeinfra - S", 900000000000000000000000),
            // ("Pier Two - S", 14760135043275578924872),
            // ("HashKey - EL", 69437369796766463147276),
            // ("Renzo by HashKey - EL", 10883131153919398733700),
            // ("Nansen - S", 294564876302483611190),
            // ("HashKey - S", 168256409516052913726),
            // ("cp0x -EL", 170485841296433353676226),
            // ("InfraSingularity - S", 14456000000000000000),
            // ("Othentic #1 - EL", 1077263220933274452699),
            // ("Ryabina - S", 102917347599086793895),
            // ("EigenYields - EL", 900000000000000000000000),
            // ("P2P", 14760135043275611878042),
            // ("Stake Capital", 169940640321278592737),
            // ("Stakely", 0),
            // ("Luganodes", 153817743348528136574701),
            // ("Mantle", 40333000000000000000000),
            // ("Node Monster", 8799687809794400248337),
            // ("Meria", 0),
            // ("A41", 0),

            ("Moonli - EL", 1200106638318368259568),
            ("Alchemy - S", 143841265124105291721),
            ("Blockscape - S", 281231310642842331985),
            ("InfraSingularity - EL", 174354829318562564437),
            ("tokenTraxx - EL", 55208658604892560125),
            ("Nodeinfra - S", 700000000000000000000000),
            ("Pier Two - S", 17898026439554614951936),
            ("HashKey - EL", 68314712703022638704039),
            ("Renzo by HashKey - EL", 10887855328100034383127),
            ("Nansen - S", 294564876302483611190),
            ("HashKey - S", 168256409516052913726),
            ("cp0x -EL", 167152265539596709179232),
            ("InfraSingularity - S", 14456000000000000000),
            ("Othentic #1 - EL", 1077263220933274452699),
            ("Ryabina - S", 94480095282109946344),
            ("EigenYields - EL", 700000000000000000000000),
            ("P2P", 17898026439554647905106),
            ("Stake Capital", 157717896570890087252),
            ("Stakely", 0),
            ("Luganodes", 153846660948029728313091),
            ("Mantle", 40333000000000000000000),
            ("Node Monster", 9061402541766949090266),
            ("Meria", 0),
            ("A41", 0),
                    
            // ("Moonli - EL", 1200106638318368259568),
            // ("Alchemy - S", 143841265124105291721),
            // ("Blockscape - S", 281231310642842331985),
            // ("InfraSingularity - EL", 174354829318562564437),
            // ("tokenTraxx - EL", 55208658604892560125),
            // ("Nodeinfra - S", 100000000000000000000000),
            // ("Pier Two - S", 17898026439554614951936),
            // ("HashKey - EL", 68314712703022638704039),
            // ("Renzo by HashKey - EL", 10887855328100034383127),
            // ("Nansen - S", 294564876302483611190),
            // ("HashKey - S", 168256409516052913726),
            // ("cp0x -EL", 100000000000000000000000),
            // ("InfraSingularity - S", 14456000000000000000),
            // ("Othentic #1 - EL", 1077263220933274452699),
            // ("Ryabina - S", 94480095282109946344),
            // ("EigenYields - EL", 100000000000000000000000),
            // ("P2P", 17898026439554647905106),
            // ("Stake Capital", 157717896570890087252),
            // ("Stakely", 0),
            // ("Luganodes", 100000000000000000000000),
            // ("Mantle", 40333000000000000000000),
            // ("Node Monster", 9061402541766949090266),
            // ("Meria", 0),
            // ("A41", 0),

            // ("Moonli - EL", 1200106638318368259568),
            // ("Alchemy - S", 143841265124105291721),
            // ("Blockscape - S", 281231310642842331985),
            // ("InfraSingularity - EL", 174354829318562564437),
            // ("tokenTraxx - EL", 55208658604892560125),
            // ("Nodeinfra - S", 10000000000000000000000),
            // ("Pier Two - S", 10000000000000000000000),
            // ("HashKey - EL", 10000000000000000000000),
            // ("Renzo by HashKey - EL", 10000000000000000000000),
            // ("Nansen - S", 294564876302483611190),
            // ("HashKey - S", 168256409516052913726),
            // ("cp0x -EL", 10000000000000000000000),
            // ("InfraSingularity - S", 14456000000000000000),
            // ("Othentic #1 - EL", 1077263220933274452699),
            // ("Ryabina - S", 94480095282109946344),
            // ("EigenYields - EL", 10000000000000000000000),
            // ("P2P", 10000000000000000000000),
            // ("Stake Capital", 157717896570890087252),
            // ("Stakely", 0),
            // ("Luganodes", 10000000000000000000000),
            // ("Mantle", 10000000000000000000000),
            // ("Node Monster", 9061402541766949090266),
            // ("Meria", 0),
            // ("A41", 0),
        ];

        let nodes: Vec<TestNode> = data
            .iter()
            .map(|(name, weight)| TestNode {
                name,
                weight: U256::from(*weight),
            })
            .collect();

        let n = 3;
        let iterations = 1000;

        let mut master_rng = ChaCha8Rng::seed_from_u64(42);
        let mut counts_classic: HashMap<&'static str, usize> = HashMap::new();
        let mut counts_sqrt: HashMap<&'static str, usize> = HashMap::new();
        let mut counts_log: HashMap<&'static str, usize> = HashMap::new();

        for _ in 0..iterations {
            let mut seed = [0u8; 32];
            master_rng.fill_bytes(&mut seed);

            if let Some(sample) = weighted_sample_n_no_classic(nodes.clone(), n, seed) {
                for node in sample {
                    *counts_classic.entry(node.name).or_default() += 1;
                }
            }
            if let Some(sample) = weighted_sample_n_no_sqrt(nodes.clone(), n, seed) {
                for node in sample {
                    *counts_sqrt.entry(node.name).or_default() += 1;
                }
            }
            if let Some(sample) = weighted_sample_n_no_log(nodes.clone(), n, seed) {
                for node in sample {
                    *counts_log.entry(node.name).or_default() += 1;
                }
            }
        }
        let mut all_entries: Vec<_> = data.clone();
        all_entries.sort_by(|a, b| b.1.cmp(&a.1));

        println!("\nDistribution over {} iterations ({} samples each):", iterations, n);
        println!("{:<30} | {:>7} | {:>7} | {:>7}", "Node", "Classic", "√Weight", "log(1+w)");
        println!("{}", "-".repeat(65));

        for (name, _) in all_entries {
            let c = counts_classic.get(name).cloned().unwrap_or(0) as f64 * 100.0 / iterations as f64;
            let s = counts_sqrt.get(name).cloned().unwrap_or(0) as f64 * 100.0 / iterations as f64;
            let l = counts_log.get(name).cloned().unwrap_or(0) as f64 * 100.0 / iterations as f64;

            println!("{:<30} | {:>6.2}% | {:>6.2}% | {:>6.2}%", name, c, s, l);
        }
    }
}
