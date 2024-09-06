use std::sync::atomic::AtomicU64;

use rand::{rngs::StdRng, SeedableRng};

static mut TEST_RNG_SEED: AtomicU64 = AtomicU64::new(1337);

pub fn get_seed() -> u64 {
    unsafe { *TEST_RNG_SEED.get_mut() }
}

pub fn set_seed(seed: u64) {
    unsafe {
        *TEST_RNG_SEED.get_mut() = seed;
    }
}

pub fn rng() -> StdRng {
    StdRng::seed_from_u64(unsafe { *TEST_RNG_SEED.get_mut() })
}
