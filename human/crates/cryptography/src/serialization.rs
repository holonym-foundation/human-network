//! Contains modules to use serde wiht points and scalars
use serde::{de::Error as DeErr, Deserialize, Deserializer, Serializer};
pub mod point_hex {
    use super::*;
    use crate::PointTrait;
    pub fn serialize<S, T, X>(point: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        T: PointTrait<X>,
    {
        let bytes = point.encode();
        serializer.serialize_str(&hex::encode(bytes))
    }
    pub fn deserialize<'de, D, T, X>(deserializer: D) -> Result<T, D::Error>
    where
        D: Deserializer<'de>,
        T: PointTrait<X>,
    {
        let s: &str = Deserialize::deserialize(deserializer)?;
        T::from_encoded(&hex::decode(s).map_err(|_| D::Error::custom(""))?).map_err(|_| D::Error::custom("error deserializing point"))
    }
}
pub mod scalar_hex {
    use super::*;
    use crate::ScalarTrait;
    pub fn serialize<S, T>(scalar: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        T: ScalarTrait,
    {
        let bytes = scalar.to_bytes();
        serializer.serialize_str(&hex::encode(bytes))
    }
    pub fn deserialize<'de, D, T>(deserializer: D) -> Result<T, D::Error>
    where
        D: Deserializer<'de>,
        T: ScalarTrait,
    {
        let s: &str = Deserialize::deserialize(deserializer)?;
        T::from_bytes(&hex::decode(s).map_err(|_| D::Error::custom(""))?).map_err(|_| D::Error::custom("error deserializing point"))
    }
}
pub mod map_point_hex {
    use crate::PointTrait;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::collections::HashMap;

    /// Serialize a `HashMap<u128, Vec<Point>>`.
    pub fn serialize<S, T, X>(map: &HashMap<u128, Vec<T>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        T: PointTrait<X>,
    {
        let encoded_map: HashMap<_, Vec<_>> = map.iter().map(|(key, points)| (*key, points.iter().map(|p| hex::encode(p.encode())).collect())).collect();
        encoded_map.serialize(serializer)
    }

    /// Deserialize a `HashMap<u128, Vec<Point>>`.
    pub fn deserialize<'de, D, T, X>(deserializer: D) -> Result<HashMap<u128, Vec<T>>, D::Error>
    where
        D: Deserializer<'de>,
        T: PointTrait<X>,
    {
        let encoded_map: HashMap<u128, Vec<String>> = HashMap::<u128, Vec<String>>::deserialize(deserializer)?;
        encoded_map
            .into_iter()
            .map(|(key, points)| {
                let decoded_points = points
                    .into_iter()
                    .map(|hex_str| {
                        //          T::from_encoded(&hex::decode(&hex_str).map_err(|_| D::Error::custom("Invalid hex in point deserialization"))?).map_err(|_| D::Error::custom("Error deserializing point"))
                        Ok(T::from_encoded(&hex::decode(&hex_str).unwrap()).unwrap())
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok((key, decoded_points))
            })
            .collect()
    }
}

pub mod hashmap_point_hex {
    use crate::PointTrait;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::collections::HashMap;

    /// Serialize a `HashMap<u128, Point>`.
    pub fn serialize<S, T, X>(map: &HashMap<u128, T>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        T: PointTrait<X>,
    {
        let encoded_map: HashMap<_, _> = map.iter().map(|(key, point)| (*key, hex::encode(point.encode()))).collect();
        encoded_map.serialize(serializer)
    }

    /// Deserialize a `HashMap<u128, Point>`.
    pub fn deserialize<'de, D, T, X>(deserializer: D) -> Result<HashMap<u128, T>, D::Error>
    where
        D: Deserializer<'de>,
        T: PointTrait<X>,
    {
        let encoded_map: HashMap<u128, String> = HashMap::<u128, String>::deserialize(deserializer)?;
        encoded_map
            .into_iter()
            .map(|(key, hex_str)| {
                //     let decoded_point = T::from_encoded(&hex::decode(&hex_str).map_err(|_| D::Error::custom("Invalid hex"))?).map_err(|_| D::Error::custom("Error deserializing point"))?;
                let decoded_point = T::from_encoded(&hex::decode(&hex_str).unwrap()).unwrap();
                Ok((key, decoded_point))
            })
            .collect()
    }
}
pub mod field_hex {
    use super::*;
    use ff::PrimeField;
    pub fn serialize<S, T>(field: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        T: PrimeField,
    {
        let bytes = field.to_repr();
        serializer.serialize_str(&hex::encode(bytes))
    }
    pub fn deserialize<'de, D, T>(deserializer: D) -> Result<T, D::Error>
    where
        D: Deserializer<'de>,
        T: PrimeField,
    {
        let s = String::deserialize(deserializer)?;
        let bytes = hex::decode(s).map_err(D::Error::custom)?;

        let mut repr = T::Repr::default();
        let repr_bytes = repr.as_mut();

        if bytes.len() != repr_bytes.len() {
            return Err(D::Error::custom(format!("Invalid hex length: expected {}, got {}", repr_bytes.len(), bytes.len())));
        }

        repr_bytes.copy_from_slice(&bytes);

        // Convert CtOption to regular Option first
        Ok(T::from_repr(repr).unwrap())
    }
}
