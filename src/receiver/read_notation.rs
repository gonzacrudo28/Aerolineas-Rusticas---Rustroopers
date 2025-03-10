use crate::errors::error_types::ErrorTypes;
use crate::protocol::protocol_notations::{
    bytes_map::BytesMap, consistency::Consistency, value::Value,
};
use std::collections::HashMap;

/// This function receives an array of bytes and decode it to an i32.
pub fn read_int(bytes: &mut Vec<u8>) -> Result<i32, ErrorTypes> {
    if bytes.len() < 4 {
        return Err(ErrorTypes::new(300, "Int is too short".to_string()));
    }
    let mut result = 0;
    for item in bytes.iter().take(4) {
        result = result << 8 | *item as i32;
    }
    bytes.drain(0..4);
    Ok(result)
}

/// This function receives an array of bytges and decode it to an i64.
pub fn read_long(bytes: &mut Vec<u8>) -> Result<i64, ErrorTypes> {
    if bytes.len() < 8 {
        return Err(ErrorTypes::new(301, "Long is too short".to_string()));
    }
    let mut result = 0;
    for item in bytes.iter().take(8) {
        result = result << 8 | *item as i64;
    }
    bytes.drain(0..8);
    Ok(result)
}

/// This function receives an array of bytes and decode it to an u8.
pub fn read_byte(bytes: &mut Vec<u8>) -> Result<u8, ErrorTypes> {
    if bytes.is_empty() {
        return Err(ErrorTypes::new(302, "Byte is too short".to_string()));
    }
    let result = bytes[0];
    bytes.drain(0..1);
    Ok(result)
}

/// This function receives an array of bytes and decode it to an u16.
pub fn read_short(bytes: &mut Vec<u8>) -> Result<u16, ErrorTypes> {
    if bytes.len() < 2 {
        return Err(ErrorTypes::new(303, "Short type is too short".to_string()));
    }
    let mut result = 0;
    for item in bytes.iter().take(2) {
        result = result << 8 | *item as u16;
    }
    bytes.drain(0..2);
    Ok(result)
}

/// This function receives an array of bytes and decode it to a String.
pub fn read_string(bytes: &mut Vec<u8>) -> Result<String, ErrorTypes> {
    let length = read_short(bytes)? as usize;
    if bytes.len() < length {
        return Err(ErrorTypes::new(304, "String is too short".to_string()));
    }
    let result = String::from_utf8(bytes.drain(0..length).collect()).unwrap();
    Ok(result)
}

/// This function receives an array of bytes and decode it to an String.
pub fn read_long_string(bytes: &mut Vec<u8>) -> Result<String, ErrorTypes> {
    let length = read_int(bytes)? as usize;
    if bytes.len() < length {
        return Err(ErrorTypes::new(305, "LongString is too short".to_string()));
    }
    let result = String::from_utf8(bytes.drain(0..length).collect()).unwrap();
    Ok(result)
}

/// This function receives an array of bytes and decode it to a Vec<String>.
pub fn read_string_list(bytes: &mut Vec<u8>) -> Result<Vec<String>, ErrorTypes> {
    let length = read_int(bytes)? as usize;
    let mut result = Vec::new();
    for _ in 0..length {
        result.push(read_string(bytes)?);
    }
    Ok(result)
}

/// This function receives an array of bytes and decode it to a tuple where the first element is the length and the second one the values.
pub fn read_bytes(bytes: &mut Vec<u8>) -> Result<(i32, Value), ErrorTypes> {
    let length = read_int(bytes)?;
    if length < 0 {
        return Ok((length, Value::Null));
    }
    if bytes.len() < length as usize {
        return Err(ErrorTypes::new(306, "Bytes is too short".to_string()));
    }
    let result = bytes.drain(0..length as usize).collect();
    Ok((length, Value::Normal(result)))
}

/// This function receives an array of bytes and decode it to a Value.
pub fn read_value(bytes: &mut Vec<u8>) -> Result<Value, ErrorTypes> {
    let value_type = read_int(bytes)?;
    if value_type < -2 {
        return Err(ErrorTypes::new(307, "Invalid ValueType length".to_string()));
    }
    if value_type == -1 {
        return Ok(Value::Null);
    }
    if value_type == -2 {
        return Ok(Value::NotSet);
    }

    Ok(Value::Normal(bytes.drain(0..value_type as usize).collect()))
}

/// This function receives an array of bytes and decode it to a short bytes.
pub fn read_short_bytes(bytes: &mut Vec<u8>) -> Result<Value, ErrorTypes> {
    let length = read_short(bytes)? as usize;
    if bytes.len() < length {
        return Err(ErrorTypes::new(308, "ShortBytes is too short".to_string()));
    }
    let result = bytes.drain(0..length).collect();
    Ok(Value::Normal(result))
}

/// This function receives an array of bytes and decode it to an inet.
pub fn read_inet(bytes: &mut Vec<u8>) -> Result<String, ErrorTypes> {
    let mut addr = read_inetaddr(bytes)?;

    addr.push(':');
    addr.push_str(read_int(bytes)?.to_string().as_str());
    Ok(addr)
}

/// This function receives an array of bytes and decode it to an inet adrress.
pub fn read_inetaddr(bytes: &mut Vec<u8>) -> Result<String, ErrorTypes> {
    let length = read_byte(bytes)? as usize;
    if length != 4 && length != 16 {
        return Err(ErrorTypes::new(
            309,
            "Invalid length for inet address".to_string(),
        ));
    }
    let mut result = String::new();
    for (i, item) in bytes.iter().enumerate().take(length) {
        result.push_str(item.to_string().as_str());
        if i != length - 1 {
            result.push('.');
        }
    }
    bytes.drain(0..length);
    Ok(result)
}

/// This function receives an array of bytes and decode it to a Consistency.
pub fn read_consistency(bytes: &mut Vec<u8>) -> Result<Consistency, ErrorTypes> {
    let byte = read_short(bytes)?;
    match byte {
        0x00 => Ok(Consistency::Any),
        0x01 => Ok(Consistency::One),
        0x02 => Ok(Consistency::Two),
        0x03 => Ok(Consistency::Three),
        0x04 => Ok(Consistency::Quorum),
        0x05 => Ok(Consistency::All),
        0x06 => Ok(Consistency::LocalQuorum),
        0x07 => Ok(Consistency::EachQuorum),
        0x08 => Ok(Consistency::Serial),
        0x09 => Ok(Consistency::LocalSerial),
        0x0A => Ok(Consistency::LocalOne),
        _ => Err(ErrorTypes::new(310, "Invalid Consistency".to_string())),
    }
}

/// This function receives an array of bytes and decode it to string map.
pub fn read_string_map(bytes: &mut Vec<u8>) -> Result<HashMap<String, String>, ErrorTypes> {
    let length = read_short(bytes)? as usize;
    let mut result = HashMap::new();
    for _ in 0..length {
        let key = read_string(bytes)?;
        let value = read_string(bytes)?;
        result.insert(key, value);
    }
    Ok(result)
}

/// This function receives an array of bytes and decode it to a hash where the key is a string and the value is a Vec<String>.
pub fn read_string_multimap(
    bytes: &mut Vec<u8>,
) -> Result<HashMap<String, Vec<String>>, ErrorTypes> {
    let length = read_short(bytes)? as usize;
    let mut result = HashMap::new();
    for _ in 0..length {
        let key = read_string(bytes)?;
        let value = read_string_list(bytes)?;
        result.insert(key, value);
    }
    Ok(result)
}

/// This function receives an array of bytes and decode it to a bytes map.
pub fn read_bytes_map(bytes: &mut Vec<u8>) -> Result<BytesMap, ErrorTypes> {
    let length = read_short(bytes)? as usize;
    let mut result = HashMap::new();
    for _ in 0..length {
        let key = read_string(bytes)?;
        let value = read_bytes(bytes)?;
        result.insert(key, value);
    }
    Ok(result)
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_int() {
        let mut bytes = vec![0x00, 0x00, 0x00, 0x01];
        assert_eq!(read_int(&mut bytes), Ok(1));
        assert_eq!(bytes.len(), 0);
    }

    #[test]
    fn test_read_short() {
        let mut bytes = vec![0x00, 0x0f];
        assert_eq!(read_short(&mut bytes), Ok(15));
        assert_eq!(bytes.len(), 0);
    }

    #[test]
    fn test_read_long() {
        let mut bytes = vec![0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00];
        assert_eq!(read_long(&mut bytes), Ok(-256));
        assert_eq!(bytes.len(), 0);
    }

    #[test]
    fn test_read_byte() {
        let mut bytes = vec![0x01];
        assert_eq!(read_byte(&mut bytes), Ok(1));
        assert_eq!(bytes.len(), 0);
    }

    #[test]
    fn test_read_string() {
        let mut bytes = vec![0x00, 0x04, 0x74, 0x65, 0x73, 0x74];
        assert_eq!(read_string(&mut bytes), Ok("test".to_string()));
        assert_eq!(bytes.len(), 0);
    }

    #[test]
    fn test_read_long_string() {
        let mut bytes = vec![
            0x00, 0x00, 0x00, 0x0C, 0x74, 0x65, 0x73, 0x74, 0x74, 0x65, 0x73, 0x74, 0x74, 0x65,
            0x73, 0x74,
        ];
        assert_eq!(read_long_string(&mut bytes), Ok("testtesttest".to_string()));
        assert_eq!(bytes.len(), 0);
    }

    #[test]
    fn test_read_string_list() {
        let mut bytes = vec![
            0x00, 0x00, 0x00, 0x02, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, 0x00, 0x04, 0x74, 0x65,
            0x73, 0x74,
        ];
        assert_eq!(
            read_string_list(&mut bytes),
            Ok(vec!["test".to_string(), "test".to_string()])
        );
        assert_eq!(bytes.len(), 0);
    }

    #[test]
    fn test_read_bytes() {
        let mut bytes = vec![0x00, 0x00, 0x00, 0x2, 0x01, 0x01];
        assert_eq!(
            read_bytes(&mut bytes),
            Ok((2, Value::Normal(vec![0x01, 0x01])))
        );
        assert_eq!(bytes.len(), 0);
    }

    #[test]
    fn test_read_bytes_empty() {
        let mut bytes = vec![0xff, 0xff, 0xff, 0xff];
        assert_eq!(read_bytes(&mut bytes), Ok((-1, Value::Null)));
        assert_eq!(bytes.len(), 0);
    }

    #[test]
    fn test_read_value() {
        let mut bytes = vec![0x00, 0x00, 0x00, 0x2, 0x01, 0x01];
        assert_eq!(read_value(&mut bytes), Ok(Value::Normal(vec![0x01, 0x01])));
        assert_eq!(bytes.len(), 0);
    }

    #[test]
    fn test_read_inet() {
        let mut bytes = vec![0x04, 0xC0, 0xA8, 0x00, 0x01, 0x00, 0x00, 0x1f, 0x90];
        assert_eq!(
            read_inet(&mut bytes).unwrap(),
            String::from("192.168.0.1:8080")
        );
    }

    #[test]
    fn test_read_string_map() {
        let mut bytes = vec![
            0x00, 0x01, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74,
        ];
        let mut map = HashMap::new();
        map.insert("test".to_string(), "test".to_string());
        assert_eq!(read_string_map(&mut bytes), Ok(map));
    }

    #[test]

    fn test_read_string_multimap() {
        let mut bytes = vec![
            0x00, 0x01, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, 0x00, 0x00, 0x00, 0x02, 0x00, 0x04,
            0x74, 0x65, 0x73, 0x74, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74,
        ];
        let mut map = HashMap::new();
        map.insert(
            "test".to_string(),
            vec!["test".to_string(), "test".to_string()],
        );
        assert_eq!(read_string_multimap(&mut bytes), Ok(map));
    }

    #[test]
    fn test_read_bytes_map() {
        let mut bytes = vec![
            0x00, 0x01, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, 0x00, 0x00, 0x00, 0x02, 0x01, 0x01,
        ];
        let mut map = HashMap::new();
        map.insert("test".to_string(), (2, Value::Normal(vec![0x01, 0x01])));
        assert_eq!(read_bytes_map(&mut bytes), Ok(map));
    }
}
