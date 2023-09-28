use crate::prelude::*;
use std::time;
use zombie_sys::*;

// a bucket is the smallest unit of memory management.
// every process() shall create a new bucket.

#[derive(Eq, PartialEq, Ord, PartialOrd, Hash, Clone, Copy, Debug, Serialize, Deserialize, Default)]
pub struct Bucket(pub usize); // maybe u32 is enough

pub struct Entry {
  pub idx: LocalNodeIndex,
  pub row: Vec<DataType>,
}

pub struct ZombieManager {
  pub seen_add: usize,
  pub seen_rm: usize,
  pub seen_materialize: usize,
  pub last_print: time::Instant,
  pub kh: KineticHanger<u32>,
  pub created_time: time::Instant,
  pub fresh_bucket: usize,
}

impl ZombieManager {
  pub fn get_bucket(&mut self) -> Bucket {
    let ret = self.fresh_bucket;
    self.fresh_bucket += 1;
    Bucket(ret)
  }

  pub fn process_records(&mut self, rs: &Records, b: Bucket) {
  }

  pub fn new() -> ZombieManager {
    ZombieManager {
      seen_add: 0,
      seen_rm: 0,
      seen_materialize: 0,
      last_print: time::Instant::now(),
      kh: KineticHanger::new(),
      created_time: time::Instant::now(),
      fresh_bucket: 0,
    }
  }
}
