#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Integer(n) => write!(f, "{}", n),
            Value::Real(r) => write!(f, "{}", r),
            Value::Text(s) => write!(f, "{}", s),
            Value::Blob(b) => write!(f, "X'{}'", hex::encode(b)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Record {
    pub values: Vec<Value>,
}

impl Record {
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut result = Vec::new();

        for value in &self.values {
            match value {
                Value::Null => {
                    result.push(0);
                }
                Value::Integer(n) => {
                    result.push(1);
                    result.extend_from_slice(&n.to_be_bytes());
                }
                Value::Real(r) => {
                    result.push(3);
                    result.extend_from_slice(&r.to_be_bytes());
                }
                Value::Text(s) => {
                    result.push(2);
                    let bytes = s.as_bytes();
                    result.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
                    result.extend_from_slice(bytes);
                }
                Value::Blob(b) => {
                    result.push(4);
                    result.extend_from_slice(&(b.len() as u32).to_be_bytes());
                    result.extend_from_slice(b);
                }
            }
        }

        result
    }

    pub fn deserialize(data: &[u8]) -> crate::storage::Result<Self> {
        let mut values = Vec::new();
        let mut pos = 0;

        while pos < data.len() {
            if pos >= data.len() {
                break;
            }

            let type_byte = data[pos];
            pos += 1;

            match type_byte {
                0 => {
                    values.push(Value::Null);
                }
                1 => {
                    if pos + 8 > data.len() {
                        return Err(crate::storage::StorageError::InvalidRecordFormat);
                    }
                    let mut bytes = [0u8; 8];
                    bytes.copy_from_slice(&data[pos..pos + 8]);
                    values.push(Value::Integer(i64::from_be_bytes(bytes)));
                    pos += 8;
                }
                2 => {
                    if pos + 4 > data.len() {
                        return Err(crate::storage::StorageError::InvalidRecordFormat);
                    }
                    let mut len_bytes = [0u8; 4];
                    len_bytes.copy_from_slice(&data[pos..pos + 4]);
                    let len = u32::from_be_bytes(len_bytes) as usize;
                    pos += 4;

                    if pos + len > data.len() {
                        return Err(crate::storage::StorageError::InvalidRecordFormat);
                    }
                    let text = String::from_utf8_lossy(&data[pos..pos + len]).to_string();
                    values.push(Value::Text(text));
                    pos += len;
                }
                3 => {
                    if pos + 8 > data.len() {
                        return Err(crate::storage::StorageError::InvalidRecordFormat);
                    }
                    let mut bytes = [0u8; 8];
                    bytes.copy_from_slice(&data[pos..pos + 8]);
                    values.push(Value::Real(f64::from_be_bytes(bytes)));
                    pos += 8;
                }
                4 => {
                    if pos + 4 > data.len() {
                        return Err(crate::storage::StorageError::InvalidRecordFormat);
                    }
                    let mut len_bytes = [0u8; 4];
                    len_bytes.copy_from_slice(&data[pos..pos + 4]);
                    let len = u32::from_be_bytes(len_bytes) as usize;
                    pos += 4;

                    if pos + len > data.len() {
                        return Err(crate::storage::StorageError::InvalidRecordFormat);
                    }
                    let blob = data[pos..pos + len].to_vec();
                    values.push(Value::Blob(blob));
                    pos += len;
                }
                _ => {
                    return Err(crate::storage::StorageError::InvalidRecordFormat);
                }
            }
        }

        Ok(Self { values })
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
            (Value::Null, _) => std::cmp::Ordering::Less,
            (_, Value::Null) => std::cmp::Ordering::Greater,
            (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
            (Value::Integer(a), Value::Real(b)) => {
                let af = *a as f64;
                af.total_cmp(b)
            }
            (Value::Real(a), Value::Integer(b)) => {
                let bf = *b as f64;
                a.total_cmp(&bf)
            }
            (Value::Real(a), Value::Real(b)) => a.total_cmp(b),
            (Value::Text(a), Value::Text(b)) => a.cmp(b),
            (Value::Blob(a), Value::Blob(b)) => a.cmp(b),
            // Type ordering: Null < Integer < Real < Text < Blob
            (Value::Integer(_), _) => std::cmp::Ordering::Less,
            (_, Value::Integer(_)) => std::cmp::Ordering::Greater,
            (Value::Real(_), _) => std::cmp::Ordering::Less,
            (_, Value::Real(_)) => std::cmp::Ordering::Greater,
            (Value::Text(_), _) => std::cmp::Ordering::Less,
            (_, Value::Text(_)) => std::cmp::Ordering::Greater,
        }
    }
}

impl Eq for Value {}

impl std::hash::Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Value::Null => 0.hash(state),
            Value::Integer(n) => {
                1.hash(state);
                n.hash(state);
            }
            Value::Real(r) => {
                2.hash(state);
                r.to_bits().hash(state);
            }
            Value::Text(s) => {
                3.hash(state);
                s.hash(state);
            }
            Value::Blob(b) => {
                4.hash(state);
                b.hash(state);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_deserialize() {
        let record = Record::new(vec![
            Value::Integer(42),
            Value::Text("hello".to_string()),
            Value::Null,
        ]);

        let serialized = record.serialize();
        let deserialized = Record::deserialize(&serialized).unwrap();

        assert_eq!(record.values, deserialized.values);
    }

    #[test]
    fn test_serialize_deserialize_with_blob() {
        let record = Record::new(vec![
            Value::Integer(42),
            Value::Blob(vec![0x01, 0x02, 0x03, 0xAB, 0xCD]),
            Value::Text("hello".to_string()),
        ]);

        let serialized = record.serialize();
        let deserialized = Record::deserialize(&serialized).unwrap();

        assert_eq!(record.values, deserialized.values);

        // Also test Display formatting for BLOB
        let blob_val = Value::Blob(vec![0x12, 0x34, 0xAB]);
        assert_eq!(format!("{}", blob_val), "X'1234ab'");
    }
}
