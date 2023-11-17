use crate::prelude::*;
use std::time::*;
use zombie_sys::*;
use std::fs::File;
use std::fs::OpenOptions;
use std::env;
use zombie_sys::Heap;
use std::collections::HashMap;
use std::collections::HashSet;
use rand::Rng;
use std::convert::TryInto;
use std::io::Write;
use serde_json::json;
use std::cell::RefCell;
use std::sync::Arc;

// a bucket is the smallest unit of memory management.
// every process() shall create a new bucket.

#[derive(Eq, PartialEq, Ord, PartialOrd, Hash, Clone, Copy, Debug, Serialize, Deserialize, Default)]
pub struct Bucket(pub usize); // maybe u32 is enough

pub struct BRecorder(pub usize);

impl BRecorder {
  pub fn new() -> BRecorder {
    BRecorder(0)
  }
}

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

pub fn duration_to_millis(d: Duration) -> u64 {
  d.as_millis().try_into().unwrap()
}

pub fn sys_time() -> u64 {
  match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
    Ok(n) => duration_to_millis(n),
    Err(_) => panic!("SystemTime before UNIX EPOCH!"),
  }
}

pub struct GD {
  pub h: Heap<KHEntry>,
  pub l: f64,
}

impl GD {
  pub fn new() -> GD {
    GD {
      h: Heap::new(),
      l: 0.0
    }
  }

  pub fn push(&mut self, entry: KHEntry, density: f64) {
    self.h.push(entry, self.l + density);
  }

  pub fn pop(&mut self) -> KHEntry {
    self.l = self.h.peek_score();
    self.h.pop()
  }

  pub fn is_empty(&self) -> bool {
    self.h.is_empty()
  }
}

pub struct ZombieManager {
  pub buffer: EvictBuffer, 
  pub kh: KineticHanger<KHEntry>,
  pub gd: GD,
  pub seen_add: usize,
  pub seen_rm: usize,
  pub seen_materialize: usize,
  pub last_print: Instant,
  pub created_time: Instant,
  pub fresh_bucket: usize,
  pub log: File,
  pub last_log_eviction: Instant,
  pub time_spent_eviction: Duration,
  pub last_log_recomputation: Instant,
  pub time_spent_recomputation: Duration,
  pub total_time_spent_waiting_ms: u64,
  pub last_log_waiting: Instant,
  pub num_hit: usize,
  pub num_miss: usize,
  pub last_log_process: Instant,
  pub c_value: i128,
  pub evict_record: HashMap<LocalNodeIndex, usize>,
  pub last_log_individual_eviction: Instant,
  pub last_update_size: Instant,
  pub br: BRecorder,
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

  pub fn use_kh() -> bool {
    let str = env::var("USE_KH").unwrap();
    if str == "0" {
      false
    } else if str == "1" {
      true
    } else {
      panic!()
    }
  }

  pub fn log_path() -> String {
    let dir = env::var("ZOMBIE_LOG_DIR").unwrap();
    let mut rng = rand::thread_rng();
    let n1: u32 = rng.gen();
    dir + "/" + &n1.to_string() + ".log"
  }
  
  pub fn get_time(&self) -> u128 {
    self.created_time.elapsed().as_micros()
  }

  pub fn get_bucket(&mut self) -> Bucket {
    let ret = self.fresh_bucket;
    self.fresh_bucket += 1;
    Bucket(ret)
  }

  pub fn new() -> ZombieManager {
    ZombieManager {
      buffer: EvictBuffer::new(),
      kh: KineticHanger::new(0),
      gd: GD::new(),
      seen_add: 0,
      seen_rm: 0,
      seen_materialize: 0,
      last_print: Instant::now(),
      created_time: Instant::now(),
      last_log_eviction: Instant::now(),
      last_log_recomputation: Instant::now(),
      last_log_waiting: Instant::now(),
      fresh_bucket: 0,
      log: OpenOptions::new().write(true)
			     .create_new(true)
                             .open(Self::log_path()).unwrap(),
      time_spent_eviction: Duration::ZERO,
      time_spent_recomputation: Duration::ZERO,
      total_time_spent_waiting_ms: 0,
      num_hit: 0,
      num_miss: 0,
      last_log_process: Instant::now(),
      c_value: 0,
      evict_record: HashMap::new(),
      last_log_individual_eviction: Instant::now(),
      last_update_size: Instant::now(),
      br: BRecorder::new(),
    }
  }

  pub fn write_json(&mut self, j: serde_json::Value) {
    self.log.write_all(serde_json::to_string(&j).unwrap().as_bytes()).unwrap();
    self.log.write_all(b"\n").unwrap();
  }

  pub fn record_eviction(&mut self, time: Duration) {
    self.time_spent_eviction += time;
    if (self.last_log_eviction.elapsed().as_secs() >= 1) {
      self.write_json(json!({"command": "eviction", "current_time": sys_time(), "spent_time": duration_to_millis(self.time_spent_eviction), "c_value": self.c_value}));
      self.last_log_eviction = Instant::now();
      self.time_spent_eviction = Duration::ZERO;
    }
  }

  pub fn count_downstream(&self) -> bool {
    true
  }
  
  pub fn record_recomputation(&mut self, time: Duration) {
    self.time_spent_recomputation += time;
    if (self.last_log_recomputation.elapsed().as_secs() >= 1) {
      self.write_json(json!({"command": "recomputation", "current_time": sys_time(), "spent_time": duration_to_millis(self.time_spent_recomputation)}));
      self.last_log_recomputation = Instant::now();
      self.time_spent_recomputation = Duration::ZERO;
    }
  }

  // note that record_eviction and record_recomputation pass in the current duration, while this pass in the total duration.
  pub fn record_waiting(&mut self, total_time_ms: u64) {
    if (self.last_log_waiting.elapsed().as_secs() >= 1) {
      assert!(total_time_ms >= self.total_time_spent_waiting_ms);
      self.write_json(json!({"command": "wait", "current_time": sys_time(), "spent_time": total_time_ms - self.total_time_spent_waiting_ms}));
      self.last_log_waiting = Instant::now();
      self.total_time_spent_waiting_ms = total_time_ms;
    }
  }

  pub fn record_process(&mut self, num_hit: usize, num_miss: usize) {
    if (num_hit != 0) || (num_miss != 0) {
      self.num_hit += num_hit;
      self.num_miss += num_miss;
      if (self.last_log_process.elapsed().as_secs() >= 1) {
        self.write_json(json!({"command": "process", "current_time": sys_time(), "hit": self.num_hit, "miss": self.num_miss}));
        self.last_log_process = Instant::now();
        self.num_hit = 0;
	self.num_miss = 0;
      }
    }
  }

  pub fn record_individual_eviction(&mut self, i: LocalNodeIndex, m: usize) {
    let e = self.evict_record.entry(i).or_insert(0);
    *e += m;
    if (self.last_log_individual_eviction.elapsed().as_secs() >= 5) {
      let mut breakdown = serde_json::Map::<String, serde_json::Value>::new();
      for x in &self.evict_record {
        breakdown.insert(x.0.to_string(), json!(x.1));
      }
      self.write_json(json!({"command": "log_individual_eviction", "current_time": sys_time(), "breakdown": breakdown}));
      self.last_log_individual_eviction = Instant::now();
      self.evict_record = HashMap::new();
    }
  }

  pub fn record_size(&mut self, m: usize) {
    if (self.last_update_size.elapsed().as_secs() >= 1) {
      self.write_json(json!({"command": "update_size", "current_time": sys_time(), "size": m}));
      self.last_update_size = Instant::now();
    }
  }
}
