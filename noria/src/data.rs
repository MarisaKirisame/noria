use arccstr::ArcCStr;

use chrono::{self, NaiveDate, NaiveDateTime};

use nom_sql::Literal;

use std::convert::TryFrom;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::{Add, Div, Mul, Sub};

const FLOAT_PRECISION: f64 = 1_000_000_000.0;
const TINYTEXT_WIDTH: usize = 15;

/// The main type used for user data throughout the codebase.
///
/// Having this be an enum allows for our code to be agnostic about the types of user data except
/// when type information is specifically necessary.
///
/// Note that cloning a `DataType` using the `Clone` trait is possible, but may result in cache
/// contention on the reference counts for de-duplicated strings. Use `DataType::deep_clone` to
/// clone the *value* of a `DataType` without danger of contention.
#[derive(Eq, Clone, Serialize, Deserialize)]
#[warn(variant_size_differences)]
pub enum DataType {
    /// An empty value.
    None,
    /// A signed 32-bit numeric value.
    Int(i32),
    /// An unsigned 32-bit numeric value.
    UnsignedInt(u32),
    /// A signed 64-bit numeric value.
    BigInt(i64),
    /// An unsigned signed 64-bit numeric value.
    UnsignedBigInt(u64),
    /// A fixed point real value. The first field is the integer part, while the second is the
    /// fractional and must be between -999999999 and 999999999.
    Real(i64, i32),
    /// A reference-counted string-like value.
    Text(ArcCStr),
    /// A tiny string that fits in a pointer
    TinyText([u8; TINYTEXT_WIDTH]),
    /// A timestamp for date/time types.
    Timestamp(NaiveDateTime),
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            DataType::None => write!(f, "NULL"),
            DataType::Text(..) | DataType::TinyText(..) => {
                let text: &str = self.into();
                // TODO: do we really want to produce quoted strings?
                write!(f, "\"{}\"", text)
            }
            DataType::Int(n) => write!(f, "{}", n),
            DataType::UnsignedInt(n) => write!(f, "{}", n),
            DataType::BigInt(n) => write!(f, "{}", n),
            DataType::UnsignedBigInt(n) => write!(f, "{}", n),
            DataType::Real(i, frac) => {
                if i == 0 && frac < 0 {
                    // We have to insert the negative sign ourselves.
                    write!(f, "-0.{:09}", frac.abs())
                } else {
                    write!(f, "{}.{:09}", i, frac.abs())
                }
            }
            DataType::Timestamp(ts) => write!(f, "{}", ts.format("%c")),
        }
    }
}

impl fmt::Debug for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            DataType::None => write!(f, "None"),
            DataType::Text(..) => {
                let text: &str = self.into();
                write!(f, "Text({:?})", text)
            }
            DataType::TinyText(..) => {
                let text: &str = self.into();
                write!(f, "TinyText({:?})", text)
            }
            DataType::Timestamp(ts) => write!(f, "Timestamp({:?})", ts),
            DataType::Real(..) => write!(f, "Real({})", self),
            DataType::Int(n) => write!(f, "Int({})", n),
            DataType::UnsignedInt(n) => write!(f, "UnsignedInt({})", n),
            DataType::BigInt(n) => write!(f, "BigInt({})", n),
            DataType::UnsignedBigInt(n) => write!(f, "UnsignedBigInt({})", n),
        }
    }
}

impl DataType {
    /// Clone the value contained within this `DataType`.
    ///
    /// This method crucially does not cause cache-line conflicts with the underlying data-store
    /// (i.e., the owner of `self`), at the cost of requiring additional allocation and copying.
    pub fn deep_clone(&self) -> Self {
        match *self {
            DataType::Text(ref cstr) => DataType::Text(ArcCStr::from(&**cstr)),
            ref dt => dt.clone(),
        }
    }

    /// Checks if this value is `DataType::None`.
    pub fn is_none(&self) -> bool {
        match *self {
            DataType::None => true,
            _ => false,
        }
    }

    /// Checks if this value is of an integral data type (i.e., can be converted into integral types).
    pub fn is_integer(&self) -> bool {
        match *self {
            DataType::Int(_) | DataType::BigInt(_) => true,
            _ => false,
        }
    }

    /// Checks if this value is of a real data type (i.e., can be converted into `f64`).
    pub fn is_real(&self) -> bool {
        match *self {
            DataType::Real(_, _) => true,
            _ => false,
        }
    }

    /// Checks if this value is of a string data type (i.e., can be converted into `String` and
    /// `&str`).
    pub fn is_string(&self) -> bool {
        match *self {
            DataType::Text(_) | DataType::TinyText(_) => true,
            _ => false,
        }
    }

    /// Checks if this values is of a timestamp data type.
    pub fn is_datetime(&self) -> bool {
        match *self {
            DataType::Timestamp(_) => true,
            _ => false,
        }
    }
}

impl PartialEq for DataType {
    fn eq(&self, other: &DataType) -> bool {
        unsafe {
            use std::{mem, slice};
            // if the two datatypes are byte-for-byte identical, they're the same.
            let a: &[u8] =
                slice::from_raw_parts(&*self as *const _ as *const u8, mem::size_of::<Self>());
            let b: &[u8] =
                slice::from_raw_parts(&*other as *const _ as *const u8, mem::size_of::<Self>());
            if a == b {
                return true;
            }
        }

        match (self, other) {
            (&DataType::Text(ref a), &DataType::Text(ref b)) => a == b,
            (&DataType::TinyText(ref a), &DataType::TinyText(ref b)) => a == b,
            (&DataType::Text(..), &DataType::TinyText(..))
            | (&DataType::TinyText(..), &DataType::Text(..)) => {
                let a: &str = self.into();
                let b: &str = other.into();
                a == b
            }
            (&DataType::BigInt(a), &DataType::BigInt(b)) => a == b,
            (&DataType::UnsignedBigInt(a), &DataType::UnsignedBigInt(b)) => a == b,
            (&DataType::Int(a), &DataType::Int(b)) => a == b,
            (&DataType::UnsignedInt(a), &DataType::UnsignedInt(b)) => a == b,
            (&DataType::UnsignedBigInt(..), &DataType::Int(..))
            | (&DataType::UnsignedBigInt(..), &DataType::UnsignedInt(..))
            | (&DataType::UnsignedBigInt(..), &DataType::BigInt(..))
            | (&DataType::UnsignedInt(..), &DataType::Int(..))
            | (&DataType::UnsignedInt(..), &DataType::BigInt(..))
            | (&DataType::UnsignedInt(..), &DataType::UnsignedBigInt(..))
            | (&DataType::BigInt(..), &DataType::Int(..))
            | (&DataType::BigInt(..), &DataType::UnsignedInt(..))
            | (&DataType::BigInt(..), &DataType::UnsignedBigInt(..))
            | (&DataType::Int(..), &DataType::UnsignedInt(..))
            | (&DataType::Int(..), &DataType::UnsignedBigInt(..))
            | (&DataType::Int(..), &DataType::BigInt(..)) => {
                let a: i128 = self.into();
                let b: i128 = other.into();
                a == b
            }
            (&DataType::Real(ai, af), &DataType::Real(bi, bf)) => ai == bi && af == bf,
            (&DataType::Timestamp(tsa), &DataType::Timestamp(tsb)) => tsa == tsb,
            (&DataType::None, &DataType::None) => true,

            _ => false,
        }
    }
}

use std::cmp::Ordering;
impl PartialOrd for DataType {
    fn partial_cmp(&self, other: &DataType) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DataType {
    fn cmp(&self, other: &DataType) -> Ordering {
        match (self, other) {
            (&DataType::Text(ref a), &DataType::Text(ref b)) => a.cmp(b),
            (&DataType::TinyText(ref a), &DataType::TinyText(ref b)) => a.cmp(b),
            (&DataType::Text(..), &DataType::TinyText(..))
            | (&DataType::TinyText(..), &DataType::Text(..)) => {
                let a: &str = self.into();
                let b: &str = other.into();
                a.cmp(&b)
            }
            (&DataType::BigInt(a), &DataType::BigInt(ref b)) => a.cmp(b),
            (&DataType::UnsignedBigInt(a), &DataType::UnsignedBigInt(ref b)) => a.cmp(b),
            (&DataType::Int(a), &DataType::Int(b)) => a.cmp(&b),
            (&DataType::UnsignedInt(a), &DataType::UnsignedInt(b)) => a.cmp(&b),
            (&DataType::BigInt(..), &DataType::Int(..))
            | (&DataType::Int(..), &DataType::BigInt(..))
            | (&DataType::BigInt(..), &DataType::UnsignedInt(..))
            | (&DataType::UnsignedInt(..), &DataType::BigInt(..))
            | (&DataType::BigInt(..), &DataType::UnsignedBigInt(..))
            | (&DataType::UnsignedBigInt(..), &DataType::BigInt(..))
            | (&DataType::UnsignedBigInt(..), &DataType::UnsignedInt(..))
            | (&DataType::UnsignedInt(..), &DataType::UnsignedBigInt(..))
            | (&DataType::Int(..), &DataType::UnsignedBigInt(..))
            | (&DataType::UnsignedBigInt(..), &DataType::Int(..))
            | (&DataType::UnsignedInt(..), &DataType::Int(..))
            | (&DataType::Int(..), &DataType::UnsignedInt(..)) => {
                let a: i128 = self.into();
                let b: i128 = other.into();
                a.cmp(&b)
            }
            (&DataType::Real(ai, af), &DataType::Real(ref bi, ref bf)) => {
                ai.cmp(bi).then_with(|| af.cmp(bf))
            }
            (&DataType::Timestamp(tsa), &DataType::Timestamp(ref tsb)) => tsa.cmp(tsb),
            (&DataType::None, &DataType::None) => Ordering::Equal,

            // order Ints, Reals, Text, Timestamps, None
            (&DataType::Int(..), _)
            | (&DataType::UnsignedInt(..), _)
            | (&DataType::BigInt(..), _)
            | (&DataType::UnsignedBigInt(..), _) => Ordering::Greater,
            (&DataType::Real(..), _) => Ordering::Greater,
            (&DataType::Text(..), _) | (&DataType::TinyText(..), _) => Ordering::Greater,
            (&DataType::Timestamp(..), _) => Ordering::Greater,
            (&DataType::None, _) => Ordering::Greater,
        }
    }
}

impl Hash for DataType {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // The default derived hash function also hashes the variant tag, which turns out to be
        // rather expensive. This version could (but probably won't) have a higher rate of
        // collisions, but the decreased overhead is worth it.
        match *self {
            DataType::None => {}
            DataType::Int(..) | DataType::BigInt(..) => {
                let n: i64 = self.into();
                n.hash(state)
            }
            DataType::UnsignedInt(..) | DataType::UnsignedBigInt(..) => {
                let n: u64 = self.into();
                n.hash(state)
            }
            DataType::Real(i, f) => {
                i.hash(state);
                f.hash(state);
            }
            DataType::Text(..) | DataType::TinyText(..) => {
                let t: &str = self.into();
                t.hash(state)
            }
            DataType::Timestamp(ts) => ts.hash(state),
        }
    }
}

impl<T> From<Option<T>> for DataType
where
    DataType: From<T>,
{
    fn from(opt: Option<T>) -> Self {
        match opt {
            Some(t) => DataType::from(t),
            None => DataType::None,
        }
    }
}

impl From<i128> for DataType {
    fn from(s: i128) -> Self {
        if s >= std::i64::MIN.into() && s <= std::i64::MAX.into() {
            DataType::BigInt(s as i64)
        } else {
            panic!("can't fit {} in a DataType::BigInt", s)
        }
    }
}

impl From<i64> for DataType {
    fn from(s: i64) -> Self {
        DataType::BigInt(s)
    }
}

impl From<u64> for DataType {
    fn from(s: u64) -> Self {
        DataType::UnsignedBigInt(s)
    }
}

impl From<i32> for DataType {
    fn from(s: i32) -> Self {
        DataType::Int(s)
    }
}

impl From<u32> for DataType {
    fn from(s: u32) -> Self {
        DataType::UnsignedInt(s)
    }
}

impl From<usize> for DataType {
    fn from(s: usize) -> Self {
        DataType::UnsignedBigInt(s as u64)
    }
}

impl From<f32> for DataType {
    fn from(f: f32) -> Self {
        Self::from(f as f64)
    }
}

impl From<f64> for DataType {
    fn from(f: f64) -> Self {
        if !f.is_finite() {
            panic!();
        }

        let mut i = f.trunc() as i64;
        let mut frac = (f.fract() * FLOAT_PRECISION).round() as i32;
        if frac == 1_000_000_000 {
            i += 1;
            frac = 0;
        } else if frac == -1_000_000_000 {
            i -= 1;
            frac = 0;
        }

        DataType::Real(i, frac)
    }
}

impl<'a> From<&'a DataType> for DataType {
    fn from(dt: &'a DataType) -> Self {
        dt.clone()
    }
}

impl<'a> From<&'a Literal> for DataType {
    fn from(l: &'a Literal) -> Self {
        match *l {
            Literal::Null => DataType::None,
            Literal::Integer(i) => (i as i64).into(),
            Literal::String(ref s) => s.as_str().into(),
            Literal::CurrentTimestamp => {
                let ts = chrono::Local::now().naive_local();
                DataType::Timestamp(ts)
            }
            Literal::FixedPoint(ref r) => {
                DataType::Real(i64::from(r.integral), r.fractional as i32)
            }
            _ => unimplemented!(),
        }
    }
}

impl From<Literal> for DataType {
    fn from(l: Literal) -> Self {
        (&l).into()
    }
}

impl From<NaiveDateTime> for DataType {
    fn from(dt: NaiveDateTime) -> Self {
        DataType::Timestamp(dt)
    }
}

// This conversion has many unwraps, but all of them are expected to be safe,
// because DataType variants (i.e. `Text` and `TinyText`) constructors are all
// generated from valid UTF-8 strings, or the constructor fails (e.g. TryFrom &[u8]).
// Thus, we can safely generate a &str from a DataType.
impl<'a> From<&'a DataType> for &'a str {
    fn from(data: &'a DataType) -> Self {
        match *data {
            DataType::Text(ref s) => s.to_str().unwrap(),
            DataType::TinyText(ref bts) => {
                if bts[TINYTEXT_WIDTH - 1] == 0 {
                    // NULL terminated CStr
                    use std::ffi::CStr;
                    let null = bts.iter().position(|&i| i == 0).unwrap() + 1;
                    CStr::from_bytes_with_nul(&bts[0..null])
                        .unwrap()
                        .to_str()
                        .unwrap()
                } else {
                    // String is exactly eight bytes
                    std::str::from_utf8(bts).unwrap()
                }
            }
            _ => panic!("attempted to convert a {:?} to a string", data),
        }
    }
}

impl From<DataType> for i128 {
    fn from(data: DataType) -> Self {
        (&data).into()
    }
}

impl From<DataType> for i64 {
    fn from(data: DataType) -> i64 {
        (&data).into()
    }
}

impl From<&'_ DataType> for i128 {
    fn from(data: &'_ DataType) -> Self {
        match *data {
            DataType::BigInt(s) => i128::from(s),
            DataType::UnsignedBigInt(s) => i128::from(s),
            DataType::Int(s) => i128::from(s),
            DataType::UnsignedInt(s) => i128::from(s),
            _ => panic!("attempted to convert a {:?} to an i128", data),
        }
    }
}

impl From<&'_ DataType> for i64 {
    fn from(data: &'_ DataType) -> Self {
        match *data {
            DataType::BigInt(s) => s,
            DataType::Int(s) => i64::from(s),
            DataType::UnsignedInt(s) => i64::from(s),
            _ => panic!("attempted to convert a {:?} to an i64", data),
        }
    }
}

impl From<DataType> for u64 {
    fn from(data: DataType) -> Self {
        (&data).into()
    }
}

impl From<&'_ DataType> for u64 {
    fn from(data: &'_ DataType) -> Self {
        match *data {
            DataType::UnsignedBigInt(s) => s,
            DataType::UnsignedInt(s) => u64::from(s),
            _ => panic!("attempted to convert a {:?} to a u64", data),
        }
    }
}

impl From<DataType> for i32 {
    fn from(data: DataType) -> Self {
        (&data).into()
    }
}

impl From<&'_ DataType> for i32 {
    fn from(data: &'_ DataType) -> Self {
        if let DataType::Int(s) = *data {
            s
        } else {
            panic!("attempted to convert a {:?} to a i32", data)
        }
    }
}

impl From<DataType> for u32 {
    fn from(data: DataType) -> Self {
        (&data).into()
    }
}

impl From<&'_ DataType> for u32 {
    fn from(data: &'_ DataType) -> Self {
        if let DataType::UnsignedInt(s) = *data {
            s
        } else {
            panic!("attempted to convert a {:?} to a u32", data)
        }
    }
}

impl From<DataType> for f64 {
    fn from(data: DataType) -> Self {
        (&data).into()
    }
}

impl From<&'_ DataType> for f64 {
    fn from(data: &'_ DataType) -> Self {
        match *data {
            DataType::Real(i, f) => i as f64 + f64::from(f) / FLOAT_PRECISION,
            DataType::Int(i) => f64::from(i),
            DataType::BigInt(i) => i as f64,
            _ => panic!("attempted to convert a {:?} to an f64", data),
        }
    }
}

impl From<String> for DataType {
    fn from(s: String) -> Self {
        DataType::try_from(s.as_bytes()).unwrap()
    }
}

impl<'a> From<&'a str> for DataType {
    fn from(s: &'a str) -> Self {
        DataType::try_from(s.as_bytes()).unwrap()
    }
}

impl<'a> TryFrom<&'a [u8]> for DataType {
    type Error = &'static str;

    fn try_from(b: &[u8]) -> Result<Self, Self::Error> {
        let len = b.len();
        if len <= TINYTEXT_WIDTH {
            let mut bytes = [0; TINYTEXT_WIDTH];
            if len != 0 {
                let bts = &mut bytes[0..len];
                bts.copy_from_slice(b);
            }

            Ok(DataType::TinyText(bytes))
        } else {
            match ArcCStr::try_from(b) {
                Ok(arc_c_str) => Ok(DataType::Text(arc_c_str)),
                Err(_) => Err("Invalid utf-8 string"),
            }
        }
    }
}

impl TryFrom<mysql_common::value::Value> for DataType {
    type Error = &'static str;

    fn try_from(v: mysql_common::value::Value) -> Result<Self, Self::Error> {
        use mysql_common::value::Value;

        match v {
            Value::NULL => Ok(DataType::None),
            Value::Bytes(v) => DataType::try_from(&v[..]),
            Value::Int(v) => Ok(v.into()),
            Value::UInt(v) => Ok(v.into()),
            Value::Float(v) => Ok(v.into()),
            Value::Double(v) => Ok(v.into()),
            Value::Date(year, month, day, hour, minutes, seconds, micros) => {
                Ok(DataType::Timestamp(
                    NaiveDate::from_ymd(year.into(), month.into(), day.into()).and_hms_micro(
                        hour.into(),
                        minutes.into(),
                        seconds.into(),
                        micros.into(),
                    ),
                ))
            }
            Value::Time(..) => Err("`mysql_common::value::Value::time` is not supported in Noria"),
        }
    }
}

// Performs an arithmetic operation on two numeric DataTypes,
// returning a new DataType as the result.
macro_rules! arithmetic_operation (
    ($op:tt, $first:ident, $second:ident) => (
        match ($first, $second) {
            (&DataType::None, _) | (_, &DataType::None) => DataType::None,
            (&DataType::Int(a), &DataType::Int(b)) => (a $op b).into(),
            (&DataType::UnsignedInt(a), &DataType::UnsignedInt(b)) => (a $op b).into(),
            (&DataType::BigInt(a), &DataType::BigInt(b)) => (a $op b).into(),
            (&DataType::UnsignedBigInt(a), &DataType::UnsignedBigInt(b)) => (a $op b).into(),

            (&DataType::Int(a), &DataType::BigInt(b)) => (i64::from(a) $op b).into(),
            (&DataType::BigInt(a), &DataType::Int(b)) => (a $op i64::from(b)).into(),
            (&DataType::Int(a), &DataType::UnsignedBigInt(b)) => (i128::from(a) $op i128::from(b)).into(),
            (&DataType::UnsignedBigInt(a), &DataType::Int(b)) => (i128::from(a) $op i128::from(b)).into(),
            (&DataType::BigInt(a), &DataType::UnsignedBigInt(b)) => (i128::from(a) $op i128::from(b)).into(),
            (&DataType::UnsignedBigInt(a), &DataType::BigInt(b)) => (i128::from(a) $op i128::from(b)).into(),
            (&DataType::UnsignedBigInt(a), &DataType::UnsignedInt(b)) => (a $op u64::from(b)).into(),
            (&DataType::UnsignedInt(a), &DataType::UnsignedBigInt(b)) => (u64::from(a) $op b).into(),

            (first @ &DataType::Int(..), second @ &DataType::Real(..)) |
            (first @ &DataType::BigInt(..), second @ &DataType::Real(..)) |
            (first @ &DataType::UnsignedInt(..), second @ &DataType::Real(..)) |
            (first @ &DataType::UnsignedBigInt(..), second @ &DataType::Real(..)) |
            (first @ &DataType::Real(..), second @ &DataType::Int(..)) |
            (first @ &DataType::Real(..), second @ &DataType::BigInt(..)) |
            (first @ &DataType::Real(..), second @ &DataType::UnsignedInt(..)) |
            (first @ &DataType::Real(..), second @ &DataType::UnsignedBigInt(..)) |
            (first @ &DataType::Real(..), second @ &DataType::Real(..)) => {
                let a: f64 = first.into();
                let b: f64 = second.into();
                (a $op b).into()
            }
            (first, second) => panic!(
                
                    "can't {} a {:?} and {:?}",
                    stringify!($op),
                    first,
                    second,
                
            ),
        }
    );
);

impl<'a, 'b> Add<&'b DataType> for &'a DataType {
    type Output = DataType;

    fn add(self, other: &'b DataType) -> DataType {
        arithmetic_operation!(+, self, other)
    }
}

impl<'a, 'b> Sub<&'b DataType> for &'a DataType {
    type Output = DataType;

    fn sub(self, other: &'b DataType) -> DataType {
        arithmetic_operation!(-, self, other)
    }
}

impl<'a, 'b> Mul<&'b DataType> for &'a DataType {
    type Output = DataType;

    fn mul(self, other: &'b DataType) -> DataType {
        arithmetic_operation!(*, self, other)
    }
}

impl<'a, 'b> Div<&'b DataType> for &'a DataType {
    type Output = DataType;

    fn div(self, other: &'b DataType) -> DataType {
        arithmetic_operation!(/, self, other)
    }
}

/// A modification to make to an existing value.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum Operation {
    /// Add the given value to the existing one.
    Add,
    /// Subtract the given value from the existing value.
    Sub,
}

/// A modification to make to a column in an existing row.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum Modification {
    /// Set the cell to this value.
    Set(DataType),
    /// Use the given [`Operation`] to combine the existing value and this one.
    Apply(Operation, DataType),
    /// Leave the existing value as-is.
    None,
}

impl<T> From<T> for Modification
where
    T: Into<DataType>,
{
    fn from(t: T) -> Modification {
        Modification::Set(t.into())
    }
}

/// An operation to apply to a base table.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum TableOperation {
    /// Insert the contained row.
    Insert(Vec<DataType>),
    /// Delete a row with the contained key.
    Delete {
        /// The key.
        key: Vec<DataType>,
    },
    /// If a row exists with the same key as the contained row, update it using `update`, otherwise
    /// insert `row`.
    InsertOrUpdate {
        /// This row will be inserted if no existing row is found.
        row: Vec<DataType>,
        /// These modifications will be applied to the columns of an existing row.
        update: Vec<Modification>,
    },
    /// Update an existing row with the given `key`.
    Update {
        /// The modifications to make to each column of the existing row.
        set: Vec<Modification>,
        /// The key used to identify the row to update.
        key: Vec<DataType>,
    },
}

impl TableOperation {
    #[doc(hidden)]
    pub fn row(&self) -> Option<&[DataType]> {
        match *self {
            TableOperation::Insert(ref r) => Some(r),
            TableOperation::InsertOrUpdate { ref row, .. } => Some(row),
            _ => None,
        }
    }
}

impl From<Vec<DataType>> for TableOperation {
    fn from(other: Vec<DataType>) -> Self {
        TableOperation::Insert(other)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mysql_value_to_datatype() {
        use assert_approx_eq::assert_approx_eq;
        use mysql_common::value::Value;

        // Test Value::NULL.
        let a = Value::NULL;
        let a_dt = DataType::try_from(a);
        assert!(a_dt.is_ok());
        assert_eq!(a_dt.unwrap(), DataType::None);

        // Test Value::Bytes.
        // Can't build a CString with interior nul-terminated chars.
        let a = Value::Bytes(vec![0; 30]);
        let a_dt = DataType::try_from(a);
        assert!(a_dt.is_err());

        let a = Value::Bytes(vec![1; 30]);
        let a_dt = DataType::try_from(a);
        assert!(a_dt.is_ok());
        assert_eq!(
            a_dt.unwrap(),
            DataType::Text(ArcCStr::try_from(&vec![1; 30][..]).unwrap())
        );

        let s = [1; 15];
        let a = Value::Bytes(s.to_vec());
        let a_dt = DataType::try_from(a);
        assert!(a_dt.is_ok());
        assert_eq!(a_dt.unwrap(), DataType::TinyText(s));

        // Test Value::Int.
        let a = Value::Int(-5);
        let a_dt = DataType::try_from(a);
        assert!(a_dt.is_ok());
        assert_eq!(a_dt.unwrap(), DataType::BigInt(-5));

        // Test Value::Float.
        let a = Value::UInt(5);
        let a_dt = DataType::try_from(a);
        assert!(a_dt.is_ok());
        assert_eq!(a_dt.unwrap(), DataType::UnsignedBigInt(5));

        // Test Value::Float.
        let initial_float: f32 = 4.2;
        let a = Value::Float(initial_float);
        let a_dt = DataType::try_from(a);
        assert!(a_dt.is_ok());
        let converted_float: f64 = a_dt.unwrap().into();
        assert_approx_eq!(converted_float, initial_float as f64);

        // Test Value::Date.
        let ts = NaiveDate::from_ymd(1111, 1, 11).and_hms_micro(2, 3, 4, 5);
        let a = Value::from(ts.clone());
        let a_dt = DataType::try_from(a);
        assert!(a_dt.is_ok());
        assert_eq!(a_dt.unwrap(), DataType::Timestamp(ts));

        // Test Value::Time.
        // noria::DataType has no `Time` representation.
        let a = Value::Time(true, 0, 0, 0, 0, 0);
        let a_dt = DataType::try_from(a);
        assert!(a_dt.is_err());
    }

    #[test]
    fn real_to_string() {
        let a: DataType = (2.5).into();
        let b: DataType = (-2.01).into();
        let c: DataType = (-0.012_345_678).into();
        assert_eq!(a.to_string(), "2.500000000");
        assert_eq!(b.to_string(), "-2.010000000");
        assert_eq!(c.to_string(), "-0.012345678");
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn real_to_float() {
        let original = 2.5;
        let data_type: DataType = original.into();
        let converted: f64 = (&data_type).into();
        assert_eq!(original, converted);
    }

    #[test]
    fn add_data_types() {
        assert_eq!(&DataType::from(1) + &DataType::from(2), 3.into());
        assert_eq!(&DataType::from(1.5) + &DataType::from(2), (3.5).into());
        assert_eq!(&DataType::from(2) + &DataType::from(1.5), (3.5).into());
        assert_eq!(&DataType::from(1.5) + &DataType::from(2.5), (4.0).into());
        assert_eq!(&DataType::BigInt(1) + &DataType::BigInt(2), 3.into());
        assert_eq!(&DataType::from(1) + &DataType::BigInt(2), 3.into());
        assert_eq!(&DataType::BigInt(2) + &DataType::from(1), 3.into());
    }

    #[test]
    fn subtract_data_types() {
        assert_eq!(&DataType::from(2) - &DataType::from(1), 1.into());
        assert_eq!(&DataType::from(3.5) - &DataType::from(2), (1.5).into());
        assert_eq!(&DataType::from(2) - &DataType::from(1.5), (0.5).into());
        assert_eq!(&DataType::from(3.5) - &DataType::from(2.0), (1.5).into());
        assert_eq!(&DataType::BigInt(1) - &DataType::BigInt(2), (-1).into());
        assert_eq!(&DataType::from(1) - &DataType::BigInt(2), (-1).into());
        assert_eq!(&DataType::BigInt(2) - &DataType::from(1), 1.into());
    }

    #[test]
    fn multiply_data_types() {
        assert_eq!(&DataType::from(2) * &DataType::from(1), 2.into());
        assert_eq!(&DataType::from(3.5) * &DataType::from(2), (7.0).into());
        assert_eq!(&DataType::from(2) * &DataType::from(1.5), (3.0).into());
        assert_eq!(&DataType::from(3.5) * &DataType::from(2.0), (7.0).into());
        assert_eq!(&DataType::BigInt(1) * &DataType::BigInt(2), 2.into());
        assert_eq!(&DataType::from(1) * &DataType::BigInt(2), 2.into());
        assert_eq!(&DataType::BigInt(2) * &DataType::from(1), 2.into());
    }

    #[test]
    fn divide_data_types() {
        assert_eq!(&DataType::from(2) / &DataType::from(1), 2.into());
        assert_eq!(&DataType::from(7.5) / &DataType::from(2), (3.75).into());
        assert_eq!(&DataType::from(7) / &DataType::from(2.5), (2.8).into());
        assert_eq!(&DataType::from(3.5) / &DataType::from(2.0), (1.75).into());
        assert_eq!(&DataType::BigInt(4) / &DataType::BigInt(2), 2.into());
        assert_eq!(&DataType::from(4) / &DataType::BigInt(2), 2.into());
        assert_eq!(&DataType::BigInt(4) / &DataType::from(2), 2.into());
    }

    #[test]
    #[should_panic(expected = "can't + a TinyText(\"hi\") and Int(5)")]
    fn add_invalid_types() {
        let a: DataType = "hi".into();
        let b: DataType = 5.into();
        let _ = &a + &b;
    }

    #[test]
    fn data_type_debug() {
        let tiny_text: DataType = "hi".into();
        let text: DataType = "I contain ' and \"".into();
        let real: DataType = (-0.05).into();
        let timestamp = DataType::Timestamp(NaiveDateTime::from_timestamp(0, 42_000_000));
        let int = DataType::Int(5);
        let big_int = DataType::BigInt(5);
        assert_eq!(format!("{:?}", tiny_text), "TinyText(\"hi\")");
        assert_eq!(format!("{:?}", text), "Text(\"I contain \' and \\\"\")");
        assert_eq!(format!("{:?}", real), "Real(-0.050000000)");
        assert_eq!(
            format!("{:?}", timestamp),
            "Timestamp(1970-01-01T00:00:00.042)"
        );
        assert_eq!(format!("{:?}", int), "Int(5)");
        assert_eq!(format!("{:?}", big_int), "BigInt(5)");
    }

    #[test]
    fn data_type_display() {
        let tiny_text: DataType = "hi".into();
        let text: DataType = "this is a very long text indeed".into();
        let real: DataType = (-0.05).into();
        let timestamp = DataType::Timestamp(NaiveDateTime::from_timestamp(0, 42_000_000));
        let int = DataType::Int(5);
        let big_int = DataType::BigInt(5);
        assert_eq!(format!("{}", tiny_text), "\"hi\"");
        assert_eq!(format!("{}", text), "\"this is a very long text indeed\"");
        assert_eq!(format!("{}", real), "-0.050000000");
        assert_eq!(format!("{}", timestamp), "Thu Jan  1 00:00:00 1970");
        assert_eq!(format!("{}", int), "5");
        assert_eq!(format!("{}", big_int), "5");
    }

    #[test]
    #[allow(clippy::cognitive_complexity)]
    fn data_type_fungibility() {
        use std::convert::TryFrom;

        let txt1: DataType = "hi".into();
        let txt12: DataType = "no".into();
        let txt2: DataType = DataType::Text(ArcCStr::try_from("hi").unwrap());
        let text: DataType = "this is a very long text indeed".into();
        let text2: DataType = "this is another long text".into();
        let real: DataType = (-0.05).into();
        let real2: DataType = (-0.06).into();
        let time = DataType::Timestamp(NaiveDateTime::from_timestamp(0, 42_000_000));
        let time2 = DataType::Timestamp(NaiveDateTime::from_timestamp(1, 42_000_000));
        let shrt = DataType::Int(5);
        let shrt6 = DataType::Int(6);
        let long = DataType::BigInt(5);
        let long6 = DataType::BigInt(6);
        let ushrt = DataType::UnsignedInt(5);
        let ushrt6 = DataType::UnsignedInt(6);
        let ulong = DataType::UnsignedBigInt(5);
        let ulong6 = DataType::UnsignedBigInt(6);

        assert_eq!(txt1, txt1);
        assert_eq!(txt2, txt2);
        assert_eq!(text, text);
        assert_eq!(shrt, shrt);
        assert_eq!(long, long);
        assert_eq!(ushrt, ushrt);
        assert_eq!(ulong, ulong);
        assert_eq!(real, real);
        assert_eq!(time, time);

        // coercion
        assert_eq!(txt1, txt2);
        assert_eq!(txt2, txt1);
        assert_eq!(shrt, long);
        assert_eq!(long, shrt);
        assert_eq!(ushrt, shrt);
        assert_eq!(shrt, ushrt);
        assert_eq!(ulong, long);
        assert_eq!(long, ulong);
        assert_eq!(shrt, ulong);
        assert_eq!(ulong, shrt);
        assert_eq!(ushrt, long);
        assert_eq!(long, ushrt);
        assert_eq!(ushrt, ulong);
        assert_eq!(ulong, ushrt);

        // negation
        assert_ne!(txt1, txt12);
        assert_ne!(txt1, text);
        assert_ne!(txt1, real);
        assert_ne!(txt1, time);
        assert_ne!(txt1, shrt);
        assert_ne!(txt1, long);
        assert_ne!(txt1, ushrt);
        assert_ne!(txt1, ulong);

        assert_ne!(txt2, txt12);
        assert_ne!(txt2, text);
        assert_ne!(txt2, real);
        assert_ne!(txt2, time);
        assert_ne!(txt2, shrt);
        assert_ne!(txt2, long);
        assert_ne!(txt2, ushrt);
        assert_ne!(txt2, ulong);

        assert_ne!(text, text2);
        assert_ne!(text, txt1);
        assert_ne!(text, txt2);
        assert_ne!(text, real);
        assert_ne!(text, time);
        assert_ne!(text, shrt);
        assert_ne!(text, long);
        assert_ne!(text, ushrt);
        assert_ne!(text, ulong);

        assert_ne!(real, real2);
        assert_ne!(real, txt1);
        assert_ne!(real, txt2);
        assert_ne!(real, text);
        assert_ne!(real, time);
        assert_ne!(real, shrt);
        assert_ne!(real, long);
        assert_ne!(real, ushrt);
        assert_ne!(real, ulong);

        assert_ne!(time, time2);
        assert_ne!(time, txt1);
        assert_ne!(time, txt2);
        assert_ne!(time, text);
        assert_ne!(time, real);
        assert_ne!(time, shrt);
        assert_ne!(time, long);
        assert_ne!(time, ushrt);
        assert_ne!(time, ulong);

        assert_ne!(shrt, shrt6);
        assert_ne!(shrt, txt1);
        assert_ne!(shrt, txt2);
        assert_ne!(shrt, text);
        assert_ne!(shrt, real);
        assert_ne!(shrt, time);
        assert_ne!(shrt, long6);

        assert_ne!(long, long6);
        assert_ne!(long, txt1);
        assert_ne!(long, txt2);
        assert_ne!(long, text);
        assert_ne!(long, real);
        assert_ne!(long, time);
        assert_ne!(long, shrt6);

        assert_ne!(ushrt, ushrt6);
        assert_ne!(ushrt, txt1);
        assert_ne!(ushrt, txt2);
        assert_ne!(ushrt, text);
        assert_ne!(ushrt, real);
        assert_ne!(ushrt, time);
        assert_ne!(ushrt, ulong6);
        assert_ne!(ushrt, shrt6);
        assert_ne!(ushrt, long6);

        assert_ne!(ulong, ulong6);
        assert_ne!(ulong, txt1);
        assert_ne!(ulong, txt2);
        assert_ne!(ulong, text);
        assert_ne!(ulong, real);
        assert_ne!(ulong, time);
        assert_ne!(ulong, ushrt6);
        assert_ne!(ulong, shrt6);
        assert_ne!(ulong, long6);

        use std::cmp::Ordering;
        assert_eq!(txt1.cmp(&txt1), Ordering::Equal);
        assert_eq!(txt2.cmp(&txt2), Ordering::Equal);
        assert_eq!(text.cmp(&text), Ordering::Equal);
        assert_eq!(shrt.cmp(&shrt), Ordering::Equal);
        assert_eq!(long.cmp(&long), Ordering::Equal);
        assert_eq!(real.cmp(&real), Ordering::Equal);
        assert_eq!(time.cmp(&time), Ordering::Equal);

        // coercion
        assert_eq!(txt1.cmp(&txt2), Ordering::Equal);
        assert_eq!(txt2.cmp(&txt1), Ordering::Equal);
        assert_eq!(shrt.cmp(&long), Ordering::Equal);
        assert_eq!(long.cmp(&shrt), Ordering::Equal);

        // negation
        assert_ne!(txt1.cmp(&txt12), Ordering::Equal);
        assert_ne!(txt1.cmp(&text), Ordering::Equal);
        assert_ne!(txt1.cmp(&real), Ordering::Equal);
        assert_ne!(txt1.cmp(&time), Ordering::Equal);
        assert_ne!(txt1.cmp(&shrt), Ordering::Equal);
        assert_ne!(txt1.cmp(&long), Ordering::Equal);

        assert_ne!(txt2.cmp(&txt12), Ordering::Equal);
        assert_ne!(txt2.cmp(&text), Ordering::Equal);
        assert_ne!(txt2.cmp(&real), Ordering::Equal);
        assert_ne!(txt2.cmp(&time), Ordering::Equal);
        assert_ne!(txt2.cmp(&shrt), Ordering::Equal);
        assert_ne!(txt2.cmp(&long), Ordering::Equal);

        assert_ne!(text.cmp(&text2), Ordering::Equal);
        assert_ne!(text.cmp(&txt1), Ordering::Equal);
        assert_ne!(text.cmp(&txt2), Ordering::Equal);
        assert_ne!(text.cmp(&real), Ordering::Equal);
        assert_ne!(text.cmp(&time), Ordering::Equal);
        assert_ne!(text.cmp(&shrt), Ordering::Equal);
        assert_ne!(text.cmp(&long), Ordering::Equal);

        assert_ne!(real.cmp(&real2), Ordering::Equal);
        assert_ne!(real.cmp(&txt1), Ordering::Equal);
        assert_ne!(real.cmp(&txt2), Ordering::Equal);
        assert_ne!(real.cmp(&text), Ordering::Equal);
        assert_ne!(real.cmp(&time), Ordering::Equal);
        assert_ne!(real.cmp(&shrt), Ordering::Equal);
        assert_ne!(real.cmp(&long), Ordering::Equal);

        assert_ne!(time.cmp(&time2), Ordering::Equal);
        assert_ne!(time.cmp(&txt1), Ordering::Equal);
        assert_ne!(time.cmp(&txt2), Ordering::Equal);
        assert_ne!(time.cmp(&text), Ordering::Equal);
        assert_ne!(time.cmp(&real), Ordering::Equal);
        assert_ne!(time.cmp(&shrt), Ordering::Equal);
        assert_ne!(time.cmp(&long), Ordering::Equal);

        assert_ne!(shrt.cmp(&shrt6), Ordering::Equal);
        assert_ne!(shrt.cmp(&txt1), Ordering::Equal);
        assert_ne!(shrt.cmp(&txt2), Ordering::Equal);
        assert_ne!(shrt.cmp(&text), Ordering::Equal);
        assert_ne!(shrt.cmp(&real), Ordering::Equal);
        assert_ne!(shrt.cmp(&time), Ordering::Equal);
        assert_ne!(shrt.cmp(&long6), Ordering::Equal);

        assert_ne!(long.cmp(&long6), Ordering::Equal);
        assert_ne!(long.cmp(&txt1), Ordering::Equal);
        assert_ne!(long.cmp(&txt2), Ordering::Equal);
        assert_ne!(long.cmp(&text), Ordering::Equal);
        assert_ne!(long.cmp(&real), Ordering::Equal);
        assert_ne!(long.cmp(&time), Ordering::Equal);
        assert_ne!(long.cmp(&shrt6), Ordering::Equal);

        let hash = |dt: &DataType| {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut s = DefaultHasher::new();
            dt.hash(&mut s);
            s.finish()
        };

        assert_eq!(hash(&txt1), hash(&txt1));
        assert_eq!(hash(&txt2), hash(&txt2));
        assert_eq!(hash(&text), hash(&text));
        assert_eq!(hash(&shrt), hash(&shrt));
        assert_eq!(hash(&long), hash(&long));
        assert_eq!(hash(&real), hash(&real));
        assert_eq!(hash(&time), hash(&time));

        // coercion
        assert_eq!(hash(&txt1), hash(&txt2));
        assert_eq!(hash(&txt2), hash(&txt1));
        assert_eq!(hash(&shrt), hash(&long));
        assert_eq!(hash(&long), hash(&shrt));

        // negation
        assert_ne!(hash(&txt1), hash(&txt12));
        assert_ne!(hash(&txt1), hash(&text));
        assert_ne!(hash(&txt1), hash(&real));
        assert_ne!(hash(&txt1), hash(&time));
        assert_ne!(hash(&txt1), hash(&shrt));
        assert_ne!(hash(&txt1), hash(&long));

        assert_ne!(hash(&txt2), hash(&txt12));
        assert_ne!(hash(&txt2), hash(&text));
        assert_ne!(hash(&txt2), hash(&real));
        assert_ne!(hash(&txt2), hash(&time));
        assert_ne!(hash(&txt2), hash(&shrt));
        assert_ne!(hash(&txt2), hash(&long));

        assert_ne!(hash(&text), hash(&text2));
        assert_ne!(hash(&text), hash(&txt1));
        assert_ne!(hash(&text), hash(&txt2));
        assert_ne!(hash(&text), hash(&real));
        assert_ne!(hash(&text), hash(&time));
        assert_ne!(hash(&text), hash(&shrt));
        assert_ne!(hash(&text), hash(&long));

        assert_ne!(hash(&real), hash(&real2));
        assert_ne!(hash(&real), hash(&txt1));
        assert_ne!(hash(&real), hash(&txt2));
        assert_ne!(hash(&real), hash(&text));
        assert_ne!(hash(&real), hash(&time));
        assert_ne!(hash(&real), hash(&shrt));
        assert_ne!(hash(&real), hash(&long));

        assert_ne!(hash(&time), hash(&time2));
        assert_ne!(hash(&time), hash(&txt1));
        assert_ne!(hash(&time), hash(&txt2));
        assert_ne!(hash(&time), hash(&text));
        assert_ne!(hash(&time), hash(&real));
        assert_ne!(hash(&time), hash(&shrt));
        assert_ne!(hash(&time), hash(&long));

        assert_ne!(hash(&shrt), hash(&shrt6));
        assert_ne!(hash(&shrt), hash(&txt1));
        assert_ne!(hash(&shrt), hash(&txt2));
        assert_ne!(hash(&shrt), hash(&text));
        assert_ne!(hash(&shrt), hash(&real));
        assert_ne!(hash(&shrt), hash(&time));
        assert_ne!(hash(&shrt), hash(&long6));

        assert_ne!(hash(&long), hash(&long6));
        assert_ne!(hash(&long), hash(&txt1));
        assert_ne!(hash(&long), hash(&txt2));
        assert_ne!(hash(&long), hash(&text));
        assert_ne!(hash(&long), hash(&real));
        assert_ne!(hash(&long), hash(&time));
        assert_ne!(hash(&long), hash(&shrt6));
    }
}
