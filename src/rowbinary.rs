//! Reader for ClickHouse's `RowBinaryWithNamesAndTypes` output format.
//!
//! The payload is a LEB128 column count, the column names, the column types as
//! ClickHouse type strings, then rows of little-endian values with no row
//! framing. Strings, arrays and maps carry a LEB128 length prefix, so every
//! value can only be decoded with its column type, which `Ty` parses from the
//! header. Numbers arrive as their exact bits, so timestamps and values never
//! pass through text.

use anyhow::{Context, Result, bail};
use serde_json::Value as Json;

/// A column type this reader cannot decode. Callers that can fetch the same
/// rows in another format catch it to fall back.
#[derive(Debug)]
pub(crate) struct Unsupported(pub(crate) String);

impl std::fmt::Display for Unsupported {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unsupported ClickHouse column type {}", self.0)
    }
}

impl std::error::Error for Unsupported {}

/// A ClickHouse column type, reduced to what decides its binary layout.
/// `LowCardinality(T)` is serialized exactly like `T`, so it parses to `T`.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Ty {
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    UInt128,
    UInt256,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    Int256,
    Float32,
    Float64,
    BFloat16,
    Bool,
    String,
    FixedString(usize),
    /// Days since the epoch as `UInt16`.
    Date,
    /// Days since the epoch as `Int32`.
    Date32,
    /// Seconds since the epoch as `UInt32`.
    DateTime,
    /// Ticks since the epoch as `Int64`.
    DateTime64,
    /// Seconds as `Int32`.
    Time,
    /// Ticks as `Int64`.
    Time64,
    Uuid,
    IPv4,
    IPv6,
    /// Enum values with their names; `Enum8` is one byte, `Enum16` two.
    Enum8(Vec<(i16, String)>),
    Enum16(Vec<(i16, String)>),
    /// A little-endian integer of `bytes` width, scaled down by `10^scale`.
    Decimal {
        bytes: usize,
        scale: u32,
    },
    Nullable(Box<Ty>),
    Array(Box<Ty>),
    Map(Box<Ty>, Box<Ty>),
    Tuple(Vec<Ty>),
}

impl Ty {
    /// Parses a type string from a response header.
    pub(crate) fn parse(text: &str) -> Result<Ty, Unsupported> {
        let text = text.trim();
        let unsupported = || Unsupported(text.to_owned());
        let (head, args) = match text.find('(') {
            Some(open) if text.ends_with(')') => {
                (&text[..open], Some(&text[open + 1..text.len() - 1]))
            }
            Some(_) => return Err(unsupported()),
            None => (text, None),
        };
        let argument = || args.ok_or_else(unsupported);
        let ty = match head {
            "UInt8" => Ty::UInt8,
            "UInt16" => Ty::UInt16,
            "UInt32" => Ty::UInt32,
            "UInt64" => Ty::UInt64,
            "UInt128" => Ty::UInt128,
            "UInt256" => Ty::UInt256,
            "Int8" => Ty::Int8,
            "Int16" => Ty::Int16,
            "Int32" => Ty::Int32,
            "Int64" => Ty::Int64,
            "Int128" => Ty::Int128,
            "Int256" => Ty::Int256,
            "Float32" => Ty::Float32,
            "Float64" => Ty::Float64,
            "BFloat16" => Ty::BFloat16,
            "Bool" => Ty::Bool,
            "String" => Ty::String,
            "FixedString" => {
                Ty::FixedString(argument()?.trim().parse().map_err(|_| unsupported())?)
            }
            "Date" => Ty::Date,
            "Date32" => Ty::Date32,
            // Time zones only affect text output.
            "DateTime" => Ty::DateTime,
            "DateTime64" => Ty::DateTime64,
            "Time" => Ty::Time,
            "Time64" => Ty::Time64,
            "UUID" => Ty::Uuid,
            "IPv4" => Ty::IPv4,
            "IPv6" => Ty::IPv6,
            "Enum8" => Ty::Enum8(enum_values(argument()?).ok_or_else(unsupported)?),
            "Enum16" => Ty::Enum16(enum_values(argument()?).ok_or_else(unsupported)?),
            "Decimal" => {
                let args = split_arguments(argument()?);
                let [precision, scale] = args.as_slice() else {
                    return Err(unsupported());
                };
                let precision: u32 = precision.trim().parse().map_err(|_| unsupported())?;
                let bytes = match precision {
                    0..=9 => 4,
                    10..=18 => 8,
                    19..=38 => 16,
                    _ => 32,
                };
                Ty::Decimal {
                    bytes,
                    scale: scale.trim().parse().map_err(|_| unsupported())?,
                }
            }
            "Decimal32" | "Decimal64" | "Decimal128" | "Decimal256" => {
                let bytes = match head {
                    "Decimal32" => 4,
                    "Decimal64" => 8,
                    "Decimal128" => 16,
                    _ => 32,
                };
                Ty::Decimal {
                    bytes,
                    scale: argument()?.trim().parse().map_err(|_| unsupported())?,
                }
            }
            "LowCardinality" => Ty::parse(argument()?)?,
            "Nullable" => Ty::Nullable(Box::new(Ty::parse(argument()?)?)),
            "Array" => Ty::Array(Box::new(Ty::parse(argument()?)?)),
            "Map" => {
                let args = split_arguments(argument()?);
                let [key, value] = args.as_slice() else {
                    return Err(unsupported());
                };
                Ty::Map(Box::new(Ty::parse(key)?), Box::new(Ty::parse(value)?))
            }
            "Tuple" => Ty::Tuple(
                split_arguments(argument()?)
                    .into_iter()
                    .map(|element| Ty::parse(tuple_element_type(element)))
                    .collect::<Result<_, _>>()?,
            ),
            "SimpleAggregateFunction" => {
                let args = split_arguments(argument()?);
                Ty::parse(args.last().ok_or_else(unsupported)?)?
            }
            _ => return Err(unsupported()),
        };
        Ok(ty)
    }

    /// Whether `integer` can read this type.
    pub(crate) fn is_integer(&self) -> bool {
        matches!(
            self,
            Ty::UInt8
                | Ty::UInt16
                | Ty::UInt32
                | Ty::UInt64
                | Ty::Int8
                | Ty::Int16
                | Ty::Int32
                | Ty::Int64
                | Ty::Bool
                | Ty::Date
                | Ty::Date32
                | Ty::DateTime
                | Ty::DateTime64
                | Ty::Time
                | Ty::Time64
                | Ty::Enum8(_)
                | Ty::Enum16(_)
        )
    }

    /// Whether `float` can read this type.
    pub(crate) fn is_number(&self) -> bool {
        self.is_integer()
            || matches!(
                self,
                Ty::UInt128
                    | Ty::Int128
                    | Ty::Float32
                    | Ty::Float64
                    | Ty::BFloat16
                    | Ty::Decimal { .. }
            )
    }

    /// Whether `floats` can read this type.
    pub(crate) fn is_number_array(&self) -> bool {
        matches!(self, Ty::Array(inner) if inner.is_number())
    }
}

/// Splits a type's argument list at top-level commas, outside parentheses
/// and single-quoted literals.
fn split_arguments(text: &str) -> Vec<&str> {
    let mut arguments = Vec::new();
    let mut depth = 0usize;
    let mut quoted = false;
    let mut escaped = false;
    let mut start = 0;
    for (index, c) in text.char_indices() {
        if quoted {
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == '\'' {
                quoted = false;
            }
            continue;
        }
        match c {
            '\'' => quoted = true,
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                arguments.push(text[start..index].trim());
                start = index + 1;
            }
            _ => {}
        }
    }
    let last = text[start..].trim();
    if !last.is_empty() || !arguments.is_empty() {
        arguments.push(last);
    }
    arguments
}

/// Strips the optional element name from a `Tuple` argument, which is either
/// a bare type, `name Type` or `` `quoted name` Type ``.
fn tuple_element_type(element: &str) -> &str {
    let element = element.trim();
    if let Some(rest) = element.strip_prefix('`') {
        return match rest.find('`') {
            Some(close) => rest[close + 1..].trim_start(),
            None => element,
        };
    }
    let mut depth = 0usize;
    for (index, c) in element.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            ' ' if depth == 0 => return element[index + 1..].trim_start(),
            _ => {}
        }
    }
    element
}

/// Parses `'name' = value, ...` enum definitions.
fn enum_values(text: &str) -> Option<Vec<(i16, String)>> {
    split_arguments(text)
        .into_iter()
        .map(|pair| {
            let (name, value) = pair.rsplit_once('=')?;
            let name = name.trim().strip_prefix('\'')?.strip_suffix('\'')?;
            let name = name.replace("\\'", "'").replace("\\\\", "\\");
            Some((value.trim().parse().ok()?, name))
        })
        .collect()
}

/// A column from the response header.
#[derive(Clone, Debug)]
pub(crate) struct Column {
    pub(crate) name: String,
    pub(crate) ty: Ty,
}

/// Cursor over a RowBinary payload. Every read fails on a truncated payload
/// instead of panicking, since the bytes come from the network.
pub(crate) struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    /// Resumes reading at `offset`, as returned by `offset` after the header.
    pub(crate) fn at(bytes: &'a [u8], offset: usize) -> Self {
        Self { bytes, offset }
    }

    pub(crate) fn offset(&self) -> usize {
        self.offset
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.offset >= self.bytes.len()
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .filter(|end| *end <= self.bytes.len())
            .context("truncated RowBinary payload")?;
        let bytes = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(bytes)
    }

    fn fixed<const N: usize>(&mut self) -> Result<[u8; N]> {
        let bytes = self.take(N)?;
        let mut array = [0; N];
        array.copy_from_slice(bytes);
        Ok(array)
    }

    /// Reads a LEB128 length prefix.
    pub(crate) fn length(&mut self) -> Result<usize> {
        let mut value = 0usize;
        let mut shift = 0;
        loop {
            let byte = self.u8()?;
            let part = (byte & 0x7f) as usize;
            if shift >= usize::BITS || (part << shift) >> shift != part {
                bail!("invalid RowBinary length prefix");
            }
            value |= part << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
            shift += 7;
        }
    }

    pub(crate) fn string(&mut self) -> Result<&'a str> {
        let len = self.length()?;
        std::str::from_utf8(self.take(len)?).context("ClickHouse returned invalid UTF-8")
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.fixed::<1>()?[0])
    }

    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.fixed()?))
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.fixed()?))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.fixed()?))
    }

    fn i8(&mut self) -> Result<i8> {
        Ok(i8::from_le_bytes(self.fixed()?))
    }

    fn i16(&mut self) -> Result<i16> {
        Ok(i16::from_le_bytes(self.fixed()?))
    }

    fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_le_bytes(self.fixed()?))
    }

    fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_le_bytes(self.fixed()?))
    }

    fn i128(&mut self) -> Result<i128> {
        Ok(i128::from_le_bytes(self.fixed()?))
    }

    fn u128(&mut self) -> Result<u128> {
        Ok(u128::from_le_bytes(self.fixed()?))
    }

    fn f32(&mut self) -> Result<f32> {
        Ok(f32::from_le_bytes(self.fixed()?))
    }

    fn f64(&mut self) -> Result<f64> {
        Ok(f64::from_le_bytes(self.fixed()?))
    }

    /// Reads the column names and types that start every payload.
    pub(crate) fn header(&mut self) -> Result<Vec<Column>> {
        let count = self.length()?;
        let mut names = Vec::new();
        for _ in 0..count {
            names.push(self.string()?.to_owned());
        }
        let mut columns = Vec::new();
        for name in names {
            let ty =
                Ty::parse(self.string()?).with_context(|| format!("RowBinary column {name}"))?;
            columns.push(Column { name, ty });
        }
        Ok(columns)
    }

    /// Reads one integer-like value. 64-bit unsigned values above `i64::MAX`
    /// are rejected rather than wrapped.
    pub(crate) fn integer(&mut self, ty: &Ty) -> Result<i64> {
        let value = match ty {
            Ty::UInt8 | Ty::Bool => i64::from(self.u8()?),
            Ty::UInt16 | Ty::Date => i64::from(self.u16()?),
            Ty::UInt32 | Ty::DateTime => i64::from(self.u32()?),
            Ty::UInt64 => {
                i64::try_from(self.u64()?).context("integer metric value out of range")?
            }
            Ty::Int8 => i64::from(self.i8()?),
            Ty::Int16 => i64::from(self.i16()?),
            Ty::Int32 | Ty::Date32 | Ty::Time => i64::from(self.i32()?),
            Ty::Int64 | Ty::DateTime64 | Ty::Time64 => self.i64()?,
            Ty::Enum8(_) => i64::from(self.i8()?),
            Ty::Enum16(_) => i64::from(self.i16()?),
            _ => bail!("expected an integer column, found {ty:?}"),
        };
        Ok(value)
    }

    /// Reads one numeric value as a float, converting integers like JSON
    /// number parsing would.
    pub(crate) fn float(&mut self, ty: &Ty) -> Result<f64> {
        let value = match ty {
            Ty::Float32 => f64::from(self.f32()?),
            Ty::Float64 => self.f64()?,
            Ty::BFloat16 => f64::from(f32::from_bits(u32::from(self.u16()?) << 16)),
            Ty::UInt64 => self.u64()? as f64,
            Ty::UInt128 => self.u128()? as f64,
            Ty::Int128 => self.i128()? as f64,
            Ty::Decimal { bytes, scale } => {
                let unscaled = match bytes {
                    4 => f64::from(self.i32()?),
                    8 => self.i64()? as f64,
                    16 => self.i128()? as f64,
                    _ => bail!("expected a numeric column, found {ty:?}"),
                };
                unscaled / 10f64.powi(*scale as i32)
            }
            _ if ty.is_integer() => self.integer(ty)? as f64,
            _ => bail!("expected a numeric column, found {ty:?}"),
        };
        Ok(value)
    }

    /// Appends an array of numbers to `out`.
    pub(crate) fn floats(&mut self, ty: &Ty, out: &mut Vec<f64>) -> Result<()> {
        let Ty::Array(element) = ty else {
            bail!("expected a numeric array column, found {ty:?}")
        };
        let len = self.length()?;
        out.reserve(len);
        for _ in 0..len {
            out.push(self.float(element)?);
        }
        Ok(())
    }

    /// Reads one value of any supported type as JSON. Numbers keep their
    /// value; non-finite floats become the strings ClickHouse's own JSON output
    /// uses, and types without a JSON number form become strings.
    pub(crate) fn cell(&mut self, ty: &Ty) -> Result<Json> {
        let value = match ty {
            Ty::UInt8 => Json::from(self.u8()?),
            Ty::UInt16 | Ty::Date => Json::from(self.u16()?),
            Ty::UInt32 | Ty::DateTime => Json::from(self.u32()?),
            Ty::UInt64 => Json::from(self.u64()?),
            Ty::UInt128 => Json::from(self.u128()?.to_string()),
            Ty::UInt256 | Ty::Int256 => {
                let bytes = self.fixed::<32>()?;
                Json::from(format!("0x{}", hex(&bytes)))
            }
            Ty::Int8 => Json::from(self.i8()?),
            Ty::Int16 => Json::from(self.i16()?),
            Ty::Int32 | Ty::Date32 | Ty::Time => Json::from(self.i32()?),
            Ty::Int64 | Ty::DateTime64 | Ty::Time64 => Json::from(self.i64()?),
            Ty::Int128 => Json::from(self.i128()?.to_string()),
            Ty::Float32 | Ty::Float64 | Ty::BFloat16 | Ty::Decimal { .. } => {
                float_json(self.float(ty)?)
            }
            Ty::Bool => Json::from(self.u8()? != 0),
            Ty::String => Json::from(self.string()?),
            Ty::FixedString(len) => {
                let bytes = self.take(*len)?;
                let end = bytes.iter().rposition(|b| *b != 0).map_or(0, |i| i + 1);
                Json::from(
                    std::str::from_utf8(&bytes[..end])
                        .context("ClickHouse returned invalid UTF-8")?,
                )
            }
            Ty::Uuid => {
                // Serialized as two little-endian 64-bit halves, most
                // significant first.
                let high = self.u64()?;
                let low = self.u64()?;
                let mut bytes = [0u8; 16];
                bytes[..8].copy_from_slice(&high.to_be_bytes());
                bytes[8..].copy_from_slice(&low.to_be_bytes());
                Json::from(uuid::Uuid::from_bytes(bytes).to_string())
            }
            Ty::IPv4 => {
                let address = std::net::Ipv4Addr::from_bits(self.u32()?);
                Json::from(address.to_string())
            }
            Ty::IPv6 => {
                let address = std::net::Ipv6Addr::from(self.fixed::<16>()?);
                Json::from(address.to_string())
            }
            Ty::Enum8(values) | Ty::Enum16(values) => {
                let value = self.integer(ty)?;
                let name = values
                    .iter()
                    .find(|(candidate, _)| i64::from(*candidate) == value)
                    .map(|(_, name)| name.as_str())
                    .with_context(|| format!("unknown enum value {value}"))?;
                Json::from(name)
            }
            Ty::Nullable(inner) => {
                if self.u8()? != 0 {
                    Json::Null
                } else {
                    self.cell(inner)?
                }
            }
            Ty::Array(element) => {
                let len = self.length()?;
                let mut values = Vec::with_capacity(len.min(1024));
                for _ in 0..len {
                    values.push(self.cell(element)?);
                }
                Json::Array(values)
            }
            Ty::Map(key, value) => {
                let len = self.length()?;
                let mut map = serde_json::Map::new();
                for _ in 0..len {
                    let key = match self.cell(key)? {
                        Json::String(key) => key,
                        other => other.to_string(),
                    };
                    let value = self.cell(value)?;
                    map.insert(key, value);
                }
                Json::Object(map)
            }
            Ty::Tuple(elements) => {
                let mut values = Vec::with_capacity(elements.len());
                for element in elements {
                    values.push(self.cell(element)?);
                }
                Json::Array(values)
            }
        };
        Ok(value)
    }
}

/// JSON for a float, with the strings ClickHouse emits under
/// `output_format_json_quote_denormals` for values JSON cannot represent.
fn float_json(value: f64) -> Json {
    if value.is_nan() {
        Json::from("nan")
    } else if value == f64::INFINITY {
        Json::from("inf")
    } else if value == f64::NEG_INFINITY {
        Json::from("-inf")
    } else {
        Json::from(value)
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The exception text ClickHouse appends to a streamed response that fails
/// after its headers were sent, if `bytes` ends with one.
pub(crate) fn trailing_exception(bytes: &[u8]) -> Option<&str> {
    let tail = &bytes[bytes.len().saturating_sub(4096)..];
    let start = tail.windows(6).rposition(|window| window == b"Code: ")?;
    let text = std::str::from_utf8(&tail[start..]).ok()?;
    text.contains("DB::Exception").then_some(text.trim_end())
}

#[cfg(test)]
#[path = "../tests/unit/rowbinary.rs"]
mod tests;
