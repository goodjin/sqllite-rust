#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Integer(i64),
    Text(String),
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
                Value::Text(s) => {
                    result.push(2);
                    let bytes = s.as_bytes();
                    result.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
                    result.extend_from_slice(bytes);
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
                _ => {
                    return Err(crate::storage::StorageError::InvalidRecordFormat);
                }
            }
        }

        Ok(Self { values })
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
}
