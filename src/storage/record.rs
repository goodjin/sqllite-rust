#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
    Vector(Vec<f32>),
}

impl Value {
    /// Serialize a single value to bytes
    pub fn serialize(&self) -> Vec<u8> {
        let mut result = Vec::new();
        match self {
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
            Value::Vector(v) => {
                result.push(5);
                result.extend_from_slice(&(v.len() as u32).to_be_bytes());
                for x in v {
                    result.extend_from_slice(&x.to_be_bytes());
                }
            }
        }
        result
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Integer(n) => write!(f, "{}", n),
            Value::Real(r) => write!(f, "{}", r),
            Value::Text(s) => write!(f, "{}", s),
            Value::Blob(b) => write!(f, "X'{}'", hex::encode(b)),
            Value::Vector(v) => {
                write!(f, "[")?;
                for (i, x) in v.iter().enumerate() {
                    if i > 0 { write!(f, ", ")?; }
                    write!(f, "{:.4}", x)?;
                }
                write!(f, "]")
            }
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
                Value::Vector(v) => {
                    result.push(5);
                    result.extend_from_slice(&(v.len() as u32).to_be_bytes());
                    for x in v {
                        result.extend_from_slice(&x.to_be_bytes());
                    }
                }
            }
        }

        result
    }

    pub fn deserialize(data: &[u8]) -> crate::storage::Result<Self> {
        let mut values = Vec::new();
        let mut pos = 0;

        while pos < data.len() {
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
                5 => {
                    if pos + 4 > data.len() {
                        return Err(crate::storage::StorageError::InvalidRecordFormat);
                    }
                    let mut len_bytes = [0u8; 4];
                    len_bytes.copy_from_slice(&data[pos..pos + 4]);
                    let len = u32::from_be_bytes(len_bytes) as usize;
                    pos += 4;

                    if pos + len * 4 > data.len() {
                        return Err(crate::storage::StorageError::InvalidRecordFormat);
                    }
                    let mut vector = Vec::with_capacity(len);
                    for _ in 0..len {
                        let mut bytes = [0u8; 4];
                        bytes.copy_from_slice(&data[pos..pos + 4]);
                        vector.push(f32::from_be_bytes(bytes));
                        pos += 4;
                    }
                    values.push(Value::Vector(vector));
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
            // Same types
            (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
            (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
            (Value::Real(a), Value::Real(b)) => a.total_cmp(b),
            (Value::Text(a), Value::Text(b)) => a.cmp(b),
            (Value::Blob(a), Value::Blob(b)) => a.cmp(b),
            (Value::Vector(a), Value::Vector(b)) => {
                match a.len().cmp(&b.len()) {
                    std::cmp::Ordering::Equal => {
                        for (x, y) in a.iter().zip(b.iter()) {
                            match x.total_cmp(y) {
                                std::cmp::Ordering::Equal => continue,
                                other => return other,
                            }
                        }
                        std::cmp::Ordering::Equal
                    }
                    other => other,
                }
            }

            // Cross-type comparisons for numbers
            (Value::Integer(a), Value::Real(b)) => (*a as f64).total_cmp(b),
            (Value::Real(a), Value::Integer(b)) => a.total_cmp(&(*b as f64)),

            // Type ordering: Null < Integer < Real < Text < Blob < Vector
            (Value::Null, _) => std::cmp::Ordering::Less,
            (_, Value::Null) => std::cmp::Ordering::Greater,

            (Value::Integer(_), _) => std::cmp::Ordering::Less,
            (_, Value::Integer(_)) => std::cmp::Ordering::Greater,

            (Value::Real(_), _) => std::cmp::Ordering::Less,
            (_, Value::Real(_)) => std::cmp::Ordering::Greater,

            (Value::Text(_), _) => std::cmp::Ordering::Less,
            (_, Value::Text(_)) => std::cmp::Ordering::Greater,

            (Value::Blob(_), _) => std::cmp::Ordering::Less,
            (_, Value::Blob(_)) => std::cmp::Ordering::Greater,
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
            Value::Vector(v) => {
                5.hash(state);
                v.len().hash(state);
                for x in v {
                    x.to_bits().hash(state);
                }
            }
        }
    }
}

impl std::ops::Add for Value {
    type Output = Value;
    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Value::Integer(a), Value::Integer(b)) => Value::Integer(a + b),
            (Value::Integer(a), Value::Real(b)) => Value::Real(a as f64 + b),
            (Value::Real(a), Value::Integer(b)) => Value::Real(a + b as f64),
            (Value::Real(a), Value::Real(b)) => Value::Real(a + b),
            _ => Value::Null,
        }
    }
}

impl std::ops::Sub for Value {
    type Output = Value;
    fn sub(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Value::Integer(a), Value::Integer(b)) => Value::Integer(a - b),
            (Value::Integer(a), Value::Real(b)) => Value::Real(a as f64 - b),
            (Value::Real(a), Value::Integer(b)) => Value::Real(a - b as f64),
            (Value::Real(a), Value::Real(b)) => Value::Real(a - b),
            _ => Value::Null,
        }
    }
}

impl std::ops::Mul for Value {
    type Output = Value;
    fn mul(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Value::Integer(a), Value::Integer(b)) => Value::Integer(a * b),
            (Value::Integer(a), Value::Real(b)) => Value::Real(a as f64 * b),
            (Value::Real(a), Value::Integer(b)) => Value::Real(a * b as f64),
            (Value::Real(a), Value::Real(b)) => Value::Real(a * b),
            _ => Value::Null,
        }
    }
}

impl std::ops::Div for Value {
    type Output = Value;
    fn div(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Value::Integer(a), Value::Integer(b)) => {
                if b == 0 { Value::Null } else { Value::Integer(a / b) }
            }
            (Value::Integer(a), Value::Real(b)) => Value::Real(a as f64 / b),
            (Value::Real(a), Value::Integer(b)) => Value::Real(a / b as f64),
            (Value::Real(a), Value::Real(b)) => Value::Real(a / b),
            _ => Value::Null,
        }
    }
}
