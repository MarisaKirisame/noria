use crate::prelude::*;
use std::time;
use zombie_sys::*;
use std::fs::File;
use std::fs::OpenOptions;
use std::env;
use zombie_sys::KineticHeap;
use std::collections::HashMap;
use std::collections::HashSet;

// a bucket is the smallest unit of memory management.
// every process() shall create a new bucket.

#[derive(Eq, PartialEq, Ord, PartialOrd, Hash, Clone, Copy, Debug, Serialize, Deserialize, Default)]
pub struct Bucket(pub usize); // maybe u32 is enough

pub struct KHEntry {
  pub idx: LocalNodeIndex,
  pub b: Bucket,
  pub mem: usize,  
}

pub struct EvictEntry {
  pub b: HashSet<Bucket>,
  pub mem: usize
}

pub struct EvictBuffer {
  pub map: HashMap<LocalNodeIndex, EvictEntry>
}

impl EvictBuffer {
  pub fn new() -> EvictBuffer {
    EvictBuffer {
      map: HashMap::new(),
    }
  }
}

pub struct ZombieManager {
  pub buffer: EvictBuffer, 
  pub kh: KineticHanger<KHEntry>,
  pub seen_add: usize,
  pub seen_rm: usize,
  pub seen_materialize: usize,
  pub last_print: time::Instant,
  pub created_time: time::Instant,
  pub fresh_bucket: usize,
  pub log: File,
}

impl ZombieManager {
  pub fn use_zombie() -> bool {
    let str = env::var("USE_ZOMBIE").unwrap();
    if str == "0" {
      false
    } else if str == "1" {
      true
    } else {
      panic!()
    }
  }

  pub fn get_time(&self) -> u128 {
    self.created_time.elapsed().as_millis()
  }

  pub fn get_bucket(&mut self) -> Bucket {
    let ret = self.fresh_bucket;
    self.fresh_bucket += 1;
    Bucket(ret)
  }

  pub fn process_records(&mut self, rs: &Records, b: Bucket) {
  }

  pub fn new() -> ZombieManager {
    ZombieManager {
      buffer: EvictBuffer::new(),
      kh: KineticHanger::new(0),
      seen_add: 0,
      seen_rm: 0,
      seen_materialize: 0,
      last_print: time::Instant::now(),
      created_time: time::Instant::now(),
      fresh_bucket: 0,
      log: OpenOptions::new().write(true)
			     .create(true)
                             .truncate(true)
                             .open("zombie.log").unwrap(),
    }
  }
}
