use std::{error::Error, hash::{DefaultHasher, Hash, Hasher}};

use arrow::{array::{Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Float16Array, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, StringArray, StringViewArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array}, datatypes::DataType};

use crate::ThresholdValue;

pub struct BloomFilter {
    bit_array: Vec<bool>,
    size: usize,
    num_hashes: usize,
}

impl BloomFilter {
    pub fn new(size: usize, num_hashes: usize) -> Self {
        BloomFilter {
            bit_array: vec![false; size],
            size,
            num_hashes,
        }
    }

    pub fn insert<T: Hash>(&mut self, item: &T) {
        for index in self.hash(item) {
            self.bit_array[index] = true;
        }
    }

    pub fn contains(&self, item: &ThresholdValue) -> bool {
        match item {
            ThresholdValue::Number(value) => self.hash(&value.to_bits()).iter().all(|&index| self.bit_array[index]),
            ThresholdValue::Boolean(value) => self.hash(&value).iter().all(|&index| self.bit_array[index]),
            ThresholdValue::Utf8String(value) => self.hash(value).iter().all(|&index| self.bit_array[index]),
        }
    }

    pub fn populate_from_column(&mut self, column: &dyn Array) -> Result<(), Box<dyn Error>> {
        match column.data_type() {
            DataType::Int8 => {
                let col = match column
                    .as_any()
                    .downcast_ref::<Int8Array>() {
                        Some(v) => v,
                        None => return Err("Failed to downcast Int8".into())
                    };
                for i in 0..col.len() {
                    if col.is_valid(i) {
                        let val = col.value(i);
                        self.insert(&val);
                    }
                }
            },
            DataType::Int16 => {
                let col = match column
                    .as_any()
                    .downcast_ref::<Int16Array>() {
                        Some(v) => v,
                        None => return Err("Failed to downcast Int16".into())
                    };
                for i in 0..col.len() {
                    if col.is_valid(i) {
                        let val = col.value(i);
                        self.insert(&val);
                    }
                }
            },
            DataType::Int32 => {
                let col = match column
                    .as_any()
                    .downcast_ref::<Int32Array>() {
                        Some(v) => v,
                        None => return Err("Failed to downcast Int32".into())
                    };
                for i in 0..col.len() {
                    if col.is_valid(i) {
                        let val = col.value(i);
                        self.insert(&val);
                    }
                }
            },
            DataType::Int64 => {
                let col = match column
                    .as_any()
                    .downcast_ref::<Int64Array>() {
                        Some(v) => v,
                        None => return Err("Failed to downcast Int64".into())
                    };
                for i in 0..col.len() {
                    if col.is_valid(i) {
                        let val = col.value(i);
                        self.insert(&val);
                    }
                }
            },
            DataType::Utf8 => {
                let col = match column
                    .as_any()
                    .downcast_ref::<StringArray>() {
                        Some(v) => v,
                        None => return Err("Failed to downcast String".into())
                    };
                for i in 0..col.len() {
                    if col.is_valid(i) {
                        let val = col.value(i);
                        self.insert(&val);
                    }
                }
            },
            DataType::UInt8 => {
                let col = match column
                    .as_any()
                    .downcast_ref::<UInt8Array>() {
                        Some(v) => v,
                        None => return Err("Failed to downcast UInt8".into())
                    };
                for i in 0..col.len() {
                    if col.is_valid(i) {
                        let val = col.value(i);
                        self.insert(&val);
                    }
                }
            },
            DataType::UInt16 => {
                let col = match column
                    .as_any()
                    .downcast_ref::<UInt16Array>() {
                        Some(v) => v,
                        None => return Err("Failed to downcast UInt16".into())
                    };
                for i in 0..col.len() {
                    if col.is_valid(i) {
                        let val = col.value(i);
                        self.insert(&val);
                    }
                }
            },
            DataType::UInt32 => {
                let col = match column
                    .as_any()
                    .downcast_ref::<UInt32Array>() {
                        Some(v) => v,
                        None => return Err("Failed to downcast UInt32".into())
                    };
                for i in 0..col.len() {
                    if col.is_valid(i) {
                        let val = col.value(i);
                        self.insert(&val);
                    }
                }
            },
            DataType::UInt64 => {
                let col = match column
                    .as_any()
                    .downcast_ref::<UInt64Array>() {
                        Some(v) => v,
                        None => return Err("Failed to downcast UInt64".into())
                    };
                for i in 0..col.len() {
                    if col.is_valid(i) {
                        let val = col.value(i);
                        self.insert(&val);
                    }
                }
            },
            DataType::Float16 => {
                let col = match column
                    .as_any()
                    .downcast_ref::<Float16Array>() {
                        Some(v) => v,
                        None => return Err("Failed to downcast Float16".into())
                    };
                for i in 0..col.len() {
                    if col.is_valid(i) {
                        let val = col.value(i).to_bits();
                        self.insert(&val);
                    }
                }
            },
            DataType::Float32 => {
                let col = match column
                    .as_any()
                    .downcast_ref::<Float32Array>() {
                        Some(v) => v,
                        None => return Err("Failed to downcast Float32".into())
                    };
                for i in 0..col.len() {
                    if col.is_valid(i) {
                        let val = col.value(i).to_bits();
                        self.insert(&val);
                    }
                }
            },
            DataType::Float64 => {
                let col = match column
                    .as_any()
                    .downcast_ref::<Float64Array>() {
                        Some(v) => v,
                        None => return Err("Failed to downcast Float64".into())
                    };
                for i in 0..col.len() {
                    if col.is_valid(i) {
                        let val = col.value(i).to_bits();
                        self.insert(&val);
                    }
                }
            },
            DataType::Date32 => {
                let col = match column
                    .as_any()
                    .downcast_ref::<Date32Array>() {
                        Some(v) => v,
                        None => return Err("Failed to downcast Date32".into())
                    };
                for i in 0..col.len() {
                    if col.is_valid(i) {
                        let val = col.value(i);
                        self.insert(&val);
                    }
                }
            },
            DataType::Date64 => {
                let col = match column
                    .as_any()
                    .downcast_ref::<Date64Array>() {
                        Some(v) => v,
                        None => return Err("Failed to downcast Date64".into())
                    };
                for i in 0..col.len() {
                    if col.is_valid(i) {
                        let val = col.value(i);
                        self.insert(&val);
                    }
                }
            },
            DataType::Boolean => {
                let col = match column
                    .as_any()
                    .downcast_ref::<BooleanArray>() {
                        Some(v) => v,
                        None => return Err("Failed to downcast Boolean".into())
                    };
                for i in 0..col.len() {
                    if col.is_valid(i) {
                        let val = col.value(i);
                        self.insert(&val);
                    }
                }
            },
            DataType::Binary => {
                let col = match column
                    .as_any()
                    .downcast_ref::<BinaryArray>() {
                        Some(v) => v,
                        None => return Err("Failed to downcast Binary".into())
                    };
                for i in 0..col.len() {
                    if col.is_valid(i) {
                        let val = col.value(i);
                        self.insert(&val);
                    }
                }
            },
            DataType::Utf8View => {
                let col = match column
                    .as_any()
                    .downcast_ref::<StringViewArray>() {
                        Some(v) => v,
                        None => return Err("Failed to downcast Utf8View".into())
                    };
                for i in 0..col.len() {
                    if col.is_valid(i) {
                        let val = col.value(i);
                        self.insert(&val);
                    }
                }
            },
            DataType::LargeUtf8 => {
                let col = match column
                    .as_any()
                    .downcast_ref::<StringArray>() {
                        Some(v) => v,
                        None => return Err("Failed to downcast LargeUtf8".into())
                    };
                for i in 0..col.len() {
                    if col.is_valid(i) {
                        let val = col.value(i);
                        self.insert(&val);
                    }
                }
            },
            DataType::LargeBinary => {
                let col = match column
                    .as_any()
                    .downcast_ref::<BinaryArray>() {
                        Some(v) => v,
                        None => return Err("Failed to downcast LargeBinary".into())
                    };
                for i in 0..col.len() {
                    if col.is_valid(i) {
                        let val = col.value(i);
                        self.insert(&val);
                    }
                }
            },
            DataType::BinaryView => {
                let col = match column
                    .as_any()
                    .downcast_ref::<BinaryArray>() {
                        Some(v) => v,
                        None => return Err("Failed to downcast BinaryView".into())
                    };
                for i in 0..col.len() {
                    if col.is_valid(i) {
                        let val = col.value(i);
                        self.insert(&val);
                    }
                }
            },
            _ => return Err("Invalid Data Type: Not Insertable".into())
        };
        Ok(())
    }

    fn hash<T: Hash>(&self, item: &T) -> Vec<usize> {
        let mut indices = Vec::with_capacity(self.num_hashes);
        let mut hasher = DefaultHasher::new();

        for i in 0..self.num_hashes {
            hasher.write_usize(i);
            item.hash(&mut hasher);
            let hash_value = hasher.finish();
            indices.push((hash_value as usize) % self.size);
            hasher = DefaultHasher::new();
        }
        indices
    }
}



