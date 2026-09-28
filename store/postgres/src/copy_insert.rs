//! Entity inserts with `COPY ... FROM STDIN (FORMAT binary)`.
//!
//! Postgres takes rows from `COPY` with less work per row than from `insert .. select from
//! unnest(..)`: no executor, no array to take apart, and numbers arrive in binary instead of as
//! text to parse. The rows are the ones `UnnestInsertQuery` writes, encoded the way Postgres
//! would receive the same values as bind parameters, so the stored values are the same.
//! Tables and values this does not cover (lists, full-text columns, `BigDecimal`, duplicates to
//! skip) keep using the insert queries.

use std::str::FromStr;

use bytes::{BufMut, BytesMut};
use diesel::result::Error as DieselError;
use graph::components::store::write::WriteChunk;
use graph::data::store::Value;
use graph::data::store::scalar;
use graph::futures03::SinkExt;
use graph::prelude::BlockNumber;

use crate::AsyncPgConnection;
use crate::relational::{ColumnType, Table};
use crate::relational_queries::InsertQuery;

/// The header of a binary `COPY` stream: signature, flags, header extension length.
const HEADER: &[u8] = b"PGCOPY\n\xff\r\n\0\0\0\0\0\0\0\0\0";
/// Microseconds from the Unix epoch to the Postgres epoch (2000-01-01).
const PG_EPOCH_MICROS: i64 = 946_684_800_000_000;
/// `int4range` flags: lower bound inclusive; upper bound infinite.
const RANGE_LB_INC: u8 = 0x02;
const RANGE_UB_INF: u8 = 0x10;

pub(crate) struct CopyInsert<'a> {
    table: &'a Table,
    columns: Vec<&'a crate::relational::Column>,
    data: BytesMut,
}

impl<'a> CopyInsert<'a> {
    /// Whether `table` can be written with `COPY`.
    pub fn supports(table: &Table) -> bool {
        !(table.immutable && table.skip_duplicates)
            && table.columns.iter().all(|column| {
                !column.is_list()
                    && column.fulltext_fields.is_none()
                    && matches!(
                        column.column_type,
                        ColumnType::String
                            | ColumnType::Enum(_)
                            | ColumnType::Int
                            | ColumnType::Int8
                            | ColumnType::Boolean
                            | ColumnType::Bytes
                            | ColumnType::BigInt
                            | ColumnType::Timestamp
                    )
            })
    }

    /// The `COPY` for `rows`; `None` when a row has a value this does not encode (the caller
    /// then uses the insert queries, which also report missing and mismatched values).
    pub fn new(table: &'a Table, rows: &'a WriteChunk<'a>) -> Option<Self> {
        if !Self::supports(table) {
            return None;
        }
        for row in rows {
            for column in table.columns.iter() {
                if !column.is_nullable() && !row.entity.contains_key(&column.field) {
                    return None;
                }
            }
        }
        let columns = InsertQuery::unique_columns(table, rows);
        let has_vid_seq = table.object.has_vid_seq();
        let fields =
            columns.len() + 1 + usize::from(table.has_causality_region) + usize::from(has_vid_seq);
        let fields = i16::try_from(fields).ok()?;

        let mut data =
            BytesMut::with_capacity(HEADER.len() + rows.len() * (16 + 24 * fields as usize));
        data.put_slice(HEADER);
        for row in rows {
            data.put_i16(fields);
            for column in &columns {
                let value = row.entity.get(&column.field).unwrap_or(&Value::Null);
                encode(&mut data, &column.column_type, value)?;
            }
            if table.immutable {
                data.put_i32(4);
                data.put_i32(row.block);
            } else {
                encode_range(&mut data, row.block, row.end);
            }
            if table.has_causality_region {
                data.put_i32(4);
                data.put_i32(row.causality_region.as_i32());
            }
            if has_vid_seq {
                data.put_i32(8);
                data.put_i64(row.entity.vid());
            }
        }
        data.put_i16(-1);
        Some(CopyInsert {
            table,
            columns,
            data,
        })
    }

    fn statement(&self) -> String {
        let mut sql = format!("copy {} (", self.table.qualified_name.as_str());
        for column in &self.columns {
            sql.push_str(&quote(column.name.as_str()));
            sql.push_str(", ");
        }
        sql.push_str(self.table.block_column().as_str());
        if self.table.has_causality_region {
            sql.push_str(", ");
            sql.push_str(crate::block_range::CAUSALITY_REGION_COLUMN);
        }
        if self.table.object.has_vid_seq() {
            sql.push_str(", vid");
        }
        sql.push_str(") from stdin (format binary)");
        sql
    }

    /// Runs the `COPY` on `conn` (in its current transaction); returns the number of rows.
    pub async fn execute(self, conn: &mut AsyncPgConnection) -> Result<usize, DieselError> {
        let statement = self.statement();
        let client = conn.raw_client();
        let convert = diesel_async::AsyncPgConnection::convert_error;
        let sink = client
            .copy_in::<_, bytes::Bytes>(statement.as_str())
            .await
            .map_err(convert)?;
        let mut sink = Box::pin(sink);
        sink.send(self.data.freeze()).await.map_err(convert)?;
        let rows = sink.as_mut().finish().await.map_err(convert)?;
        Ok(rows as usize)
    }
}

fn quote(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn put_bytes(data: &mut BytesMut, bytes: &[u8]) -> Option<()> {
    data.put_i32(i32::try_from(bytes.len()).ok()?);
    data.put_slice(bytes);
    Some(())
}

/// Encodes `value` for a column of `column_type` as Postgres' binary receive functions take it;
/// `None` for the combinations `UnnestInsertQuery` does not take either, and `BigDecimal`.
fn encode(data: &mut BytesMut, column_type: &ColumnType, value: &Value) -> Option<()> {
    match (column_type, value) {
        (_, Value::Null) => data.put_i32(-1),
        (ColumnType::String | ColumnType::Enum(_), Value::String(s)) => {
            put_bytes(data, s.as_bytes())?
        }
        (ColumnType::Int, Value::Int(i)) => {
            data.put_i32(4);
            data.put_i32(*i);
        }
        (ColumnType::Int8, Value::Int8(i)) => {
            data.put_i32(8);
            data.put_i64(*i);
        }
        (ColumnType::Int8, Value::String(s)) => {
            data.put_i32(8);
            data.put_i64(s.parse().ok()?);
        }
        (ColumnType::Boolean, Value::Bool(b)) => {
            data.put_i32(1);
            data.put_u8(u8::from(*b));
        }
        (ColumnType::Bytes, Value::Bytes(b)) => put_bytes(data, b.as_slice())?,
        (ColumnType::Bytes, Value::String(s)) => {
            put_bytes(data, scalar::Bytes::from_str(s).ok()?.as_slice())?
        }
        (ColumnType::BigInt, Value::BigInt(i)) => encode_integer(data, &i.to_string())?,
        (ColumnType::Timestamp, Value::Timestamp(ts)) => {
            data.put_i32(8);
            data.put_i64(ts.0.timestamp_micros() - PG_EPOCH_MICROS);
        }
        _ => return None,
    }
    Some(())
}

/// Encodes the integer written in decimal as `text` (an optional `-`, then digits) as a binary
/// `numeric` with scale 0, as `numeric_in` would store `text`.
fn encode_integer(data: &mut BytesMut, text: &str) -> Option<()> {
    let (negative, digits) = match text.strip_prefix('-') {
        Some(digits) => (true, digits),
        None => (false, text),
    };
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let digits = digits.trim_start_matches('0');
    // Base 10000 digits, most significant first.
    let mut groups: Vec<i16> = Vec::with_capacity(digits.len() / 4 + 1);
    let first = match digits.len() % 4 {
        0 => 4,
        n => n,
    };
    let mut start = 0;
    let mut end = first.min(digits.len());
    while start < digits.len() {
        groups.push(digits[start..end].parse().ok()?);
        start = end;
        end += 4;
    }
    let weight = groups.len() as i16 - 1;
    while groups.last() == Some(&0) {
        groups.pop();
    }
    let sign: u16 = if negative && !groups.is_empty() {
        0x4000
    } else {
        0
    };
    let weight = if groups.is_empty() { 0 } else { weight };
    data.put_i32(8 + 2 * groups.len() as i32);
    data.put_i16(groups.len() as i16);
    data.put_i16(weight);
    data.put_u16(sign);
    data.put_i16(0);
    for group in groups {
        data.put_i16(group);
    }
    Some(())
}

/// Encodes `[block, end)`, or `[block, ∞)` without an end, as a binary `int4range`.
fn encode_range(data: &mut BytesMut, block: BlockNumber, end: Option<BlockNumber>) {
    match end {
        Some(end) => {
            data.put_i32(1 + 8 + 8);
            data.put_u8(RANGE_LB_INC);
            data.put_i32(4);
            data.put_i32(block);
            data.put_i32(4);
            data.put_i32(end);
        }
        None => {
            data.put_i32(1 + 8);
            data.put_u8(RANGE_LB_INC | RANGE_UB_INF);
            data.put_i32(4);
            data.put_i32(block);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn integer(text: &str) -> Vec<u8> {
        let mut data = BytesMut::new();
        encode_integer(&mut data, text).unwrap();
        data.to_vec()
    }

    #[test]
    fn integers() {
        // len, ndigits, weight, sign, dscale, digits
        assert_eq!(integer("0"), [0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(
            integer("12345"),
            [0, 0, 0, 12, 0, 2, 0, 1, 0, 0, 0, 0, 0, 1, 0x09, 0x29]
        );
        assert_eq!(
            integer("-10000"),
            [0, 0, 0, 10, 0, 1, 0, 1, 0x40, 0, 0, 0, 0, 1]
        );
        assert_eq!(
            integer("9999"),
            [0, 0, 0, 10, 0, 1, 0, 0, 0, 0, 0, 0, 0x27, 0x0f]
        );
    }
}
