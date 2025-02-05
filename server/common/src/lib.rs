#[macro_use]
extern crate serde_derive;

mod local;
mod map;
mod records;

pub use self::local::*;
pub use self::map::*;
pub use self::records::*;
pub use noria::DataType;

pub trait SizeOf {
    fn deep_size_of(&self) -> usize {
        let ret = self.deep_size_of_impl();
	// println!("calling deep_size_of on {} with value of {}", std::any::type_name::<Self>(), ret);
	ret
    }
    fn deep_size_of_impl(&self) -> usize;
    fn size_of(&self) -> usize;
    fn is_empty(&self) -> bool;
}

impl SizeOf for DataType {
    fn deep_size_of_impl(&self) -> usize {
        use std::mem::size_of_val;

        let inner = match *self {
            DataType::Text(ref t) => size_of_val(t) + t.to_bytes().len(),
            _ => 0,
        };

        self.size_of() + inner
    }

    fn size_of(&self) -> usize {
        use std::mem::size_of;

        // doesn't include data if stored externally
        size_of::<DataType>()
    }

    fn is_empty(&self) -> bool {
        false
    }
}

impl SizeOf for Vec<DataType> {
    fn deep_size_of_impl(&self) -> usize {
        use std::mem::size_of_val;

        size_of_val(self) + self.iter().fold(0, |acc, d| acc + d.deep_size_of())
    }

    fn size_of(&self) -> usize {
        use std::mem::{size_of, size_of_val};

        size_of_val(self) + size_of::<DataType>() * self.len()
    }

    fn is_empty(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_type_mem_size() {
        use arccstr::ArcCStr;
        use chrono::NaiveDateTime;
        use std::convert::TryFrom;
        use std::mem::{size_of, size_of_val};

        let txt: DataType = DataType::Text(ArcCStr::try_from("hi").unwrap());
        let shrt = DataType::Int(5);
        let long = DataType::BigInt(5);
        let time = DataType::Timestamp(NaiveDateTime::from_timestamp(0, 42_000_000));

        let rec = vec![DataType::Int(5), "asdfasdfasdfasdf".into(), "asdf".into()];

        // DataType should always use 16 bytes itself
        assert_eq!(size_of::<DataType>(), 16);
        assert_eq!(size_of_val(&txt), 16);
        assert_eq!(size_of_val(&txt) as u64, txt.size_of());
        assert_eq!(txt.deep_size_of(), txt.size_of() + 8 + 2); // DataType + ArcCStr's ptr + 2 chars
        assert_eq!(size_of_val(&shrt), 16);
        assert_eq!(size_of_val(&long), 16);
        assert_eq!(size_of_val(&time), 16);
        assert_eq!(size_of_val(&time) as u64, time.size_of());
        assert_eq!(time.deep_size_of(), 16); // DataType + inline NaiveDateTime

        assert_eq!(size_of_val(&rec), 24);
        assert_eq!(rec.size_of(), 24 + 3 * 16);
        assert_eq!(rec.deep_size_of(), 24 + 3 * 16 + (8 + 16));
    }
}
