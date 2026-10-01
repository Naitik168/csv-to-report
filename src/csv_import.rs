//! Pure CSV parsing and validation. No I/O, so it is trivially unit-testable.
//!
//! Policy (see README "Invalid rows"):
//! * **File-level problems are fatal** and fail the import permanently (no retry,
//!   retrying cannot fix the file): empty file, missing required columns,
//!   duplicate column names, or a CSV syntax error the parser cannot recover from.
//! * **Row-level problems are not fatal**: the row is skipped, recorded with
//!   *every* reason it was rejected, and the import finishes as
//!   `completed_with_errors`. One bad price should not discard the good rows.

use std::collections::HashMap;
use std::str::FromStr;

use rust_decimal::Decimal;
use serde::Serialize;

pub const REQUIRED_COLUMNS: [&str; 6] = ["user_id", "order_id", "product", "quantity", "unit_price", "status"];
pub const ALLOWED_STATUSES: [&str; 4] = ["completed", "pending", "cancelled", "refunded"];

const MAX_ID_LEN: usize = 64;
const MAX_PRODUCT_LEN: usize = 255;
/// NUMERIC(12,2) holds at most 10 integer digits.
const MAX_UNIT_PRICE: Decimal = Decimal::from_parts(3_567_587_327, 232, 0, false, 2); // 9_999_999_999.99

/// A validated order row.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderRecord {
    /// 1-based line number in the file (the header is line 1).
    pub line: i32,
    pub customer_id: String,
    pub order_id: String,
    pub product: String,
    pub quantity: i32,
    pub unit_price: Decimal,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct FieldError {
    /// Column name, or `row` for problems with the row as a whole.
    pub field: String,
    pub message: String,
}

/// A rejected row and every reason it was rejected.
#[derive(Debug, Clone, PartialEq)]
pub struct RowError {
    pub line: i32,
    pub raw: String,
    pub errors: Vec<FieldError>,
}

#[derive(Debug, Default)]
pub struct ParsedImport {
    pub valid: Vec<OrderRecord>,
    pub invalid: Vec<RowError>,
}

impl ParsedImport {
    pub fn total_rows(&self) -> usize {
        self.valid.len() + self.invalid.len()
    }
}

/// File-level errors: the whole import fails, permanently.
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum FileError {
    #[error("the file is empty (no header row)")]
    Empty,
    #[error("missing required column(s): {0}; expected header: {expected}", expected = REQUIRED_COLUMNS.join(","))]
    MissingColumns(String),
    #[error("duplicate column name(s) in header: {0}")]
    DuplicateColumns(String),
    #[error("malformed CSV near line {line}: {message}")]
    Malformed { line: u64, message: String },
}

pub fn parse_orders(bytes: &[u8]) -> Result<ParsedImport, FileError> {
    // Tolerate a UTF-8 byte-order mark (Excel likes to add one).
    let bytes = bytes.strip_prefix(b"\xEF\xBB\xBF").unwrap_or(bytes);
    if bytes.iter().all(|b| b.is_ascii_whitespace()) {
        return Err(FileError::Empty);
    }

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true) // wrong field counts become row errors, not parser errors
        .trim(csv::Trim::All)
        .from_reader(bytes);

    let headers = reader
        .byte_headers()
        .map_err(|e| malformed(&e))?
        .iter()
        .map(|h| String::from_utf8_lossy(h).trim().to_ascii_lowercase())
        .collect::<Vec<_>>();
    let columns = column_index(&headers)?;

    let mut parsed = ParsedImport::default();
    // order_id -> line of its first valid occurrence (duplicate detection)
    let mut seen_orders: HashMap<String, i32> = HashMap::new();

    let mut record = csv::ByteRecord::new();
    loop {
        match reader.read_byte_record(&mut record) {
            Ok(true) => {}
            Ok(false) => break,
            Err(e) => return Err(malformed(&e)),
        }
        let line = record.position().map(|p| p.line()).unwrap_or(0) as i32;
        match validate_row(&record, &columns, headers.len(), &seen_orders) {
            Ok(mut order) => {
                order.line = line;
                seen_orders.insert(order.order_id.clone(), line);
                parsed.valid.push(order);
            }
            Err(errors) => parsed.invalid.push(RowError { line, raw: raw_record(&record), errors }),
        }
    }
    Ok(parsed)
}

fn malformed(e: &csv::Error) -> FileError {
    let line = e.position().map(|p| p.line()).unwrap_or(0);
    FileError::Malformed { line, message: e.to_string() }
}

fn column_index(headers: &[String]) -> Result<HashMap<&'static str, usize>, FileError> {
    if headers.iter().all(|h| h.is_empty()) {
        return Err(FileError::Empty);
    }
    let mut dupes = Vec::new();
    for (i, h) in headers.iter().enumerate() {
        if !h.is_empty() && headers[..i].contains(h) && !dupes.contains(h) {
            dupes.push(h.clone());
        }
    }
    if !dupes.is_empty() {
        return Err(FileError::DuplicateColumns(dupes.join(",")));
    }
    let mut index = HashMap::new();
    let mut missing = Vec::new();
    for col in REQUIRED_COLUMNS {
        match headers.iter().position(|h| h == col) {
            Some(i) => {
                index.insert(col, i);
            }
            None => missing.push(col),
        }
    }
    if !missing.is_empty() {
        return Err(FileError::MissingColumns(missing.join(",")));
    }
    Ok(index)
}

fn validate_row(
    record: &csv::ByteRecord,
    columns: &HashMap<&'static str, usize>,
    header_len: usize,
    seen_orders: &HashMap<String, i32>,
) -> Result<OrderRecord, Vec<FieldError>> {
    let mut errors = Vec::new();
    let mut err = |field: &str, message: String| errors.push(FieldError { field: field.into(), message });

    if record.len() != header_len {
        err("row", format!("expected {header_len} fields, found {}", record.len()));
    }

    // Fetch a required field as UTF-8 text; None if absent, not UTF-8, or containing NUL
    // (Postgres TEXT cannot store NUL, so it must be a row error rather than a DB failure).
    let get = |field: &'static str, err: &mut dyn FnMut(&str, String)| -> Option<String> {
        let raw = record.get(columns[field])?;
        match std::str::from_utf8(raw) {
            Ok(s) if s.contains('\0') => {
                err(field, "contains a NUL byte".into());
                None
            }
            Ok(s) => Some(s.trim().to_string()),
            Err(_) => {
                err(field, "is not valid UTF-8".into());
                None
            }
        }
    };

    let customer_id = get("user_id", &mut err);
    let order_id = get("order_id", &mut err);
    let product = get("product", &mut err);
    let quantity = get("quantity", &mut err);
    let unit_price = get("unit_price", &mut err);
    let status = get("status", &mut err);

    let customer_id = required_text("user_id", customer_id, MAX_ID_LEN, &mut err);
    let order_id = required_text("order_id", order_id, MAX_ID_LEN, &mut err);
    let product = required_text("product", product, MAX_PRODUCT_LEN, &mut err);

    if let Some(id) = &order_id {
        if let Some(first) = seen_orders.get(id) {
            err("order_id", format!("duplicate order_id {id:?} (first seen on line {first})"));
        }
    }

    let quantity = match quantity.as_deref() {
        None | Some("") => {
            err("quantity", "is required".into());
            None
        }
        Some(q) => match q.parse::<i32>() {
            Ok(n) if n > 0 => Some(n),
            Ok(n) => {
                err("quantity", format!("must be a positive integer, got {n}"));
                None
            }
            Err(_) => {
                err("quantity", format!("must be a positive integer, got {q:?}"));
                None
            }
        },
    };

    let unit_price = match unit_price.as_deref() {
        None | Some("") => {
            err("unit_price", "is required".into());
            None
        }
        Some(p) => match Decimal::from_str(p) {
            Ok(d) if d.is_sign_negative() && !d.is_zero() => {
                err("unit_price", format!("must not be negative, got {p}"));
                None
            }
            Ok(d) if d.scale() > 2 && d.normalize().scale() > 2 => {
                err("unit_price", format!("must have at most 2 decimal places, got {p}"));
                None
            }
            Ok(d) if d > MAX_UNIT_PRICE => {
                err("unit_price", format!("is too large, got {p}"));
                None
            }
            Ok(d) => Some(d.round_dp(2)),
            Err(_) => {
                err("unit_price", format!("must be a decimal number, got {p:?}"));
                None
            }
        },
    };

    let status = match status.as_deref().map(str::to_ascii_lowercase) {
        None => {
            err("status", "is required".into());
            None
        }
        Some(s) if s.is_empty() => {
            err("status", "is required".into());
            None
        }
        Some(s) if ALLOWED_STATUSES.contains(&s.as_str()) => Some(s),
        Some(s) => {
            err("status", format!("must be one of {}, got {s:?}", ALLOWED_STATUSES.join("|")));
            None
        }
    };

    if !errors.is_empty() {
        return Err(errors);
    }
    // All Options are Some when no error was recorded.
    Ok(OrderRecord {
        line: 0,
        customer_id: customer_id.unwrap(),
        order_id: order_id.unwrap(),
        product: product.unwrap(),
        quantity: quantity.unwrap(),
        unit_price: unit_price.unwrap(),
        status: status.unwrap(),
    })
}

fn required_text(
    field: &str,
    value: Option<String>,
    max_len: usize,
    err: &mut impl FnMut(&str, String),
) -> Option<String> {
    match value {
        None => {
            err(field, "is required".into());
            None
        }
        Some(v) if v.is_empty() => {
            err(field, "is required".into());
            None
        }
        Some(v) if v.chars().count() > max_len => {
            err(field, format!("must be at most {max_len} characters"));
            None
        }
        Some(v) => Some(v),
    }
}

/// Re-serialise a record as a CSV line so users see exactly what was rejected.
fn raw_record(record: &csv::ByteRecord) -> String {
    let mut w = csv::WriterBuilder::new().terminator(csv::Terminator::Any(b'\n')).from_writer(Vec::new());
    let _ = w.write_byte_record(record);
    let bytes = w.into_inner().unwrap_or_default();
    // NUL is replaced with U+FFFD so the raw row can be stored in a TEXT column.
    String::from_utf8_lossy(&bytes).trim_end_matches('\n').replace('\0', "\u{FFFD}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros_free::dec;

    /// Tiny helper so tests don't need an extra crate.
    mod rust_decimal_macros_free {
        pub fn dec(s: &str) -> rust_decimal::Decimal {
            s.parse().unwrap()
        }
    }

    pub const SAMPLE: &str = "\
user_id,order_id,product,quantity,unit_price,status
U001,O1001,Laptop,2,750.00,completed
U001,O1002,Mouse,3,25.00,completed
U002,O1003,Keyboard,1,80.00,completed
U002,O1004,Monitor,2,300.00,completed
U003,O1005,Headphones,4,50.00,completed
U003,O1006,Webcam,2,90.00,completed
U004,O1007,Desk,1,450.00,completed
U004,O1008,Chair,2,200.00,completed
U005,O1009,USB Cable,5,10.00,completed
U005,O1010,Mouse Pad,2,20.00,completed
U006,O1011,Keyboard,1,abc,completed
U006,O1012,Monitor,1,250.00,completed
";

    #[test]
    fn sample_file_has_eleven_valid_rows_and_rejects_o1011() {
        let parsed = parse_orders(SAMPLE.as_bytes()).unwrap();
        assert_eq!(parsed.total_rows(), 12);
        assert_eq!(parsed.valid.len(), 11);
        assert_eq!(parsed.invalid.len(), 1);

        let bad = &parsed.invalid[0];
        assert_eq!(bad.line, 12, "header is line 1, so O1011 is line 12");
        assert_eq!(bad.raw, "U006,O1011,Keyboard,1,abc,completed");
        assert_eq!(bad.errors.len(), 1);
        assert_eq!(bad.errors[0].field, "unit_price");

        let first = &parsed.valid[0];
        assert_eq!(first.line, 2);
        assert_eq!(first.customer_id, "U001");
        assert_eq!(first.unit_price, dec("750.00"));
    }

    #[test]
    fn all_errors_in_a_row_are_reported() {
        let csv = "user_id,order_id,product,quantity,unit_price,status\n,O1,,0,-5,shipped\n";
        let parsed = parse_orders(csv.as_bytes()).unwrap();
        let fields: Vec<_> = parsed.invalid[0].errors.iter().map(|e| e.field.as_str()).collect();
        assert_eq!(fields, ["user_id", "product", "quantity", "unit_price", "status"]);
    }

    #[test]
    fn duplicate_order_ids_in_same_file_are_rejected() {
        let csv =
            "user_id,order_id,product,quantity,unit_price,status\nU1,O1,A,1,1.00,completed\nU1,O1,B,1,1.00,completed\n";
        let parsed = parse_orders(csv.as_bytes()).unwrap();
        assert_eq!(parsed.valid.len(), 1);
        assert!(parsed.invalid[0].errors[0].message.contains("first seen on line 2"));
    }

    #[test]
    fn wrong_field_count_is_a_row_error() {
        let csv = "user_id,order_id,product,quantity,unit_price,status\nU1,O1,A,1\n";
        let parsed = parse_orders(csv.as_bytes()).unwrap();
        assert_eq!(parsed.invalid.len(), 1);
        assert_eq!(parsed.invalid[0].errors[0].field, "row");
    }

    #[test]
    fn column_order_case_bom_and_extra_columns_are_tolerated() {
        let csv =
            "\u{FEFF}Status,Unit_Price,Quantity,Product,Order_ID,User_ID,notes\ncompleted,9.5,2,Pen,O9,U9,hello\n";
        let parsed = parse_orders(csv.as_bytes()).unwrap();
        assert_eq!(parsed.valid.len(), 1);
        assert_eq!(parsed.valid[0].unit_price, dec("9.50"));
    }

    #[test]
    fn quoted_fields_with_commas_are_supported() {
        let csv = "user_id,order_id,product,quantity,unit_price,status\nU1,O1,\"Cable, USB-C\",1,5.00,completed\n";
        let parsed = parse_orders(csv.as_bytes()).unwrap();
        assert_eq!(parsed.valid[0].product, "Cable, USB-C");
    }

    #[test]
    fn precision_and_bounds_of_unit_price() {
        let csv = "user_id,order_id,product,quantity,unit_price,status\n\
                   U1,O1,A,1,1.005,completed\n\
                   U1,O2,A,1,1.500,completed\n\
                   U1,O3,A,1,99999999999.00,completed\n";
        let parsed = parse_orders(csv.as_bytes()).unwrap();
        assert_eq!(parsed.valid.len(), 1, "1.500 is fine (trailing zero)");
        assert_eq!(parsed.valid[0].order_id, "O2");
        assert_eq!(parsed.invalid.len(), 2);
    }

    #[test]
    fn file_level_errors_are_fatal() {
        assert_eq!(parse_orders(b"").unwrap_err(), FileError::Empty);
        assert_eq!(parse_orders(b"   \n ").unwrap_err(), FileError::Empty);
        assert!(matches!(
            parse_orders(b"user_id,order_id,product\nU1,O1,A\n").unwrap_err(),
            FileError::MissingColumns(m) if m == "quantity,unit_price,status"
        ));
        assert!(matches!(
            parse_orders(b"user_id,user_id,order_id,product,quantity,unit_price,status\n").unwrap_err(),
            FileError::DuplicateColumns(_)
        ));
    }

    #[test]
    fn header_only_file_is_a_valid_empty_import() {
        let parsed = parse_orders(b"user_id,order_id,product,quantity,unit_price,status\n").unwrap();
        assert_eq!(parsed.total_rows(), 0);
    }

    #[test]
    fn nul_byte_is_a_row_error_and_raw_record_is_storable() {
        let csv = b"user_id,order_id,product,quantity,unit_price,status\nU1,O1,Pe\0n,1,1.00,completed\n";
        let parsed = parse_orders(csv).unwrap();
        assert_eq!(parsed.invalid[0].errors[0].field, "product");
        assert!(!parsed.invalid[0].raw.contains('\0'));
    }

    #[test]
    fn non_utf8_field_is_a_row_error_not_a_crash() {
        let mut csv = b"user_id,order_id,product,quantity,unit_price,status\nU1,O1,".to_vec();
        csv.extend_from_slice(&[0xff, 0xfe]);
        csv.extend_from_slice(b",1,1.00,completed\n");
        let parsed = parse_orders(&csv).unwrap();
        assert_eq!(parsed.invalid[0].errors[0].field, "product");
    }

    #[test]
    fn max_unit_price_constant_is_correct() {
        assert_eq!(MAX_UNIT_PRICE, dec("9999999999.99"));
    }
}
