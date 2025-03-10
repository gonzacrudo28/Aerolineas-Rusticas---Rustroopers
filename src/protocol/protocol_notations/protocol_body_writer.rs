use super::consistency::Consistency;
use crate::errors::error_types::ErrorTypes;

/// This struct implements the body itself. It is used to write the body of the protocol frame.
///
/// The `ProtocolBody` struct represents the actual content (data) within a protocol frame. It stores the
/// payload of the frame as a vector of bytes (`Vec<u8>`). This struct is typically used in the process of
/// constructing or parsing protocol messages where the body holds the primary information or data being transferred.
///
/// ### Fields:
/// - `data`: A vector of bytes (`Vec<u8>`) that contains the body content of the protocol frame.
#[derive(Debug)]
pub struct ProtocolBody {
    data: Vec<u8>,
}
/// Creates a new instance of `ProtocolBody` with an empty data vector.
///
/// This implementation of the `Default` trait initializes a `ProtocolBody` struct with an empty `data` vector.
/// This is useful when you need to create an instance of `ProtocolBody` with no data initially, which can then
/// be populated later.
impl Default for ProtocolBody {
    fn default() -> Self {
        Self::new()
    }
}

impl ProtocolBody {
    /// Creates a new instance of `ProtocolBody` with an empty data vector.
    ///
    /// This function constructs a `ProtocolBody` by initializing the `data` field with an empty `Vec<u8>`.
    /// It is commonly used to create a `ProtocolBody` when no data is available initially, allowing for the
    /// body to be populated later.    
    pub fn new() -> ProtocolBody {
        ProtocolBody { data: Vec::new() }
    }

    /// Returns the length of the `data` vector in the `ProtocolBody`.
    ///
    /// This function provides the size of the body by returning the number of bytes
    /// contained in the `data` field, which is a `Vec<u8>`. It can be used to check
    /// the amount of data stored in the protocol body.
    pub fn get_length(&self) -> usize {
        self.data.len()
    }

    /// Returns a reference to the `data` vector in the `ProtocolBody`.
    ///
    /// This function provides access to the internal `data` field, which is a `Vec<u8>`.
    /// It allows other parts of the program to read the body data without taking ownership
    /// or modifying it directly.
    pub fn get_data(&self) -> &Vec<u8> {
        &self.data
    }

    /// Writes an integer to the `data` field of the `ProtocolBody`.
    ///
    /// This function converts the given integer `value` into its big-endian byte representation
    /// and appends it to the `data` vector. The `to_be_bytes()` method is used to ensure the
    /// integer is serialized in big-endian format.    
    pub fn write_int(&mut self, value: i32) {
        self.data.extend(&value.to_be_bytes())
    }

    /// Writes a 64-bit integer to the `data` field of the `ProtocolBody`.
    ///
    /// This function converts the given long integer `value` into its big-endian byte representation
    /// and appends it to the `data` vector.    
    pub fn write_long(&mut self, value: i64) {
        self.data.extend(&value.to_be_bytes())
    }

    /// Writes an 8-bit byte to the `data` field of the `ProtocolBody`.
    ///
    /// This function simply adds the given byte `value` to the `data` vector.
    pub fn write_byte(&mut self, value: u8) {
        self.data.push(value)
    }

    /// Writes a 16-bit integer to the `data` field of the `ProtocolBody`.
    ///
    /// This function converts the given short integer `value` into its big-endian byte representation
    /// and appends it to the `data` vector.
    pub fn write_short(&mut self, value: u16) {
        self.data.extend(&value.to_be_bytes())
    }

    /// Writes a string to the `data` field of the `ProtocolBody`.
    ///
    /// This function writes a string `value` to the body, first writing its length as a short integer
    /// followed by the string's bytes. The maximum string length is `u16::MAX`.
    ///
    /// Returns an error if the string is too long.    
    pub fn write_string(&mut self, value: String) -> Result<(), ErrorTypes> {
        if value.len() > u16::MAX as usize {
            return Err(ErrorTypes::new(100, "String is too long".to_string()));
        }
        self.write_short(value.len() as u16);
        self.data.extend(value.as_bytes());
        Ok(())
    }

    /// Writes a long string (with length up to `i32::MAX`) to the `data` field.
    ///
    /// The function first writes the length of the string as a 32-bit integer, followed by the string's bytes.
    ///
    /// Returns an error if the string is too long.
    pub fn write_long_string(&mut self, value: String) -> Result<(), ErrorTypes> {
        if value.len() > i32::MAX as usize {
            return Err(ErrorTypes::new(101, "Long String is too long".to_string()));
        }
        self.write_int(value.len() as i32);
        self.data.extend(value.as_bytes());
        Ok(())
    }

    /// Writes a list of strings to the `data` field.
    ///
    /// The function first writes the length of the list, then writes each string in the list.
    ///
    /// Returns an error if the list is too long or if any string in the list is too long.
    pub fn write_string_list(&mut self, values: Vec<String>) -> Result<(), ErrorTypes> {
        if values.len() > u16::MAX as usize {
            return Err(ErrorTypes::new(102, "String List is too long".to_string()));
        }
        self.write_short(values.len() as u16);
        for value in values {
            self.write_string(value)?;
        }
        Ok(())
    }

    /// Writes a list of bytes to the `data` field.
    ///
    /// The function first writes the specified number of bytes (`n`) as a 32-bit integer, then writes the
    /// byte values. If `n` is negative, no bytes are written.    
    pub fn write_bytes(&mut self, values: Vec<u8>, n: i32) -> Result<(), ErrorTypes> {
        if values.len() > i32::MAX as usize {
            return Err(ErrorTypes::new(103, "Bytes is too long".to_string()));
        }

        self.data.extend(n.to_be_bytes());
        if n < 0 {
            return Ok(());
        }
        for value in values {
            self.data.extend(value.to_be_bytes());
        }
        Ok(())
    }

    /// Writes a list of values to the `data` field.
    ///
    /// Similar to `write_bytes`, this function first writes the specified number of values (`n`), then
    /// writes each value in the list.
    ///
    /// Returns an error if the list is too long or if `n` is invalid.    
    pub fn write_value(&mut self, values: Vec<u8>, n: i32) -> Result<(), ErrorTypes> {
        if values.len() > i32::MAX as usize || n < -2 {
            return Err(ErrorTypes::new(104, "Value is too long".to_string()));
        }
        self.data.extend(n.to_be_bytes());

        for value in values {
            self.data.extend(value.to_be_bytes());
        }

        Ok(())
    }

    /// Writes a list of short bytes to the `data` field.
    ///
    /// This function writes the length of the byte list, then writes the bytes themselves.    
    pub fn write_short_bytes(&mut self, value: Vec<u8>) -> Result<(), ErrorTypes> {
        if value.len() > u16::MAX as usize {
            return Err(ErrorTypes::new(105, "Short Bytes is too long".to_string()));
        }

        self.write_short(value.len() as u16);
        self.data.extend(value);
        Ok(())
    }

    /// Writes an inet to body.
    pub fn write_inet(&mut self, address: Vec<u8>, port: i32) -> Result<(), ErrorTypes> {
        self.write_inetaddr(address)?;
        self.data.extend(port.to_be_bytes());
        Ok(())
    }

    /// Writes an inet address to body.
    pub fn write_inetaddr(&mut self, address: Vec<u8>) -> Result<(), ErrorTypes> {
        if address.len() != 4 && address.len() != 16 {
            return Err(ErrorTypes::new(106, "Inet Address is invalid".to_string()));
        }
        self.data.push(address.len() as u8);
        self.data.extend(address);
        Ok(())
    }

    /// Writes a consistency level to body.
    pub fn write_consistency(&mut self, level: Consistency) {
        self.data.extend((level as u16).to_be_bytes());
    }

    /// Writes a string map to body.
    pub fn write_string_map(&mut self, values: Vec<(String, String)>) -> Result<(), ErrorTypes> {
        if values.len() > u16::MAX as usize {
            return Err(ErrorTypes::new(107, "String Map is too long".to_string()));
        }
        self.write_short(values.len() as u16);
        for (key, value) in values {
            self.write_string(key)?;
            self.write_string(value)?;
        }
        Ok(())
    }

    /// Writes a string multimap to body.
    pub fn write_string_multimap(
        &mut self,
        values: Vec<(String, Vec<String>)>,
    ) -> Result<(), ErrorTypes> {
        if values.len() > u16::MAX as usize {
            return Err(ErrorTypes::new(
                108,
                "String MultiMap is too long".to_string(),
            ));
        }
        self.write_short(values.len() as u16);
        for (key, value) in values {
            self.write_string(key)?;
            self.write_string_list(value)?;
        }
        Ok(())
    }

    /// Returns the binary data of the body.
    pub fn get_binary(&self) -> Vec<u8> {
        self.data.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_int() {
        let mut body = ProtocolBody::new();
        body.write_int(-1);
        assert_eq!(i32::from_be_bytes(body.data.try_into().unwrap()), -1);
    }

    #[test]
    fn test_write_long() {
        let mut body = ProtocolBody::new();
        body.write_long(-100000);
        assert_eq!(i64::from_be_bytes(body.data.try_into().unwrap()), -100000);
    }

    #[test]
    fn test_write_byte() {
        let mut body = ProtocolBody::new();
        body.write_byte(0xFF);
        assert_eq!(u8::from_be_bytes(body.data.try_into().unwrap()), 255);
    }

    #[test]
    fn test_write_short() {
        let mut body = ProtocolBody::new();
        body.write_short(0x08);
        assert_eq!(u16::from_be_bytes(body.data.try_into().unwrap()), 8);
    }

    #[test]
    fn test_write_string() -> Result<(), ErrorTypes> {
        let mut body = ProtocolBody::new();
        body.write_string(String::from("Hello"))?;
        let n = u16::from_be_bytes(body.data[0..2].try_into().unwrap());
        assert_eq!(n, 5);
        assert_eq!(String::from_utf8(body.data[2..].to_vec()).unwrap(), "Hello");
        Ok(())
    }

    #[test]
    fn test_write_string_failure() -> Result<(), ()> {
        let mut body = ProtocolBody::new();
        let large_string = "a".repeat(u16::MAX as usize + 1);
        let res = body.write_string(large_string);
        assert!(res.is_err());
        Ok(())
    }

    #[test]
    fn test_write_long_string() -> Result<(), ErrorTypes> {
        let mut body = ProtocolBody::new();
        body.write_long_string(String::from("Hello World"))?;
        let n = i32::from_be_bytes(body.data[0..4].try_into().unwrap());
        assert_eq!(n, 11);
        assert_eq!(
            String::from_utf8(body.data[4..].to_vec()).unwrap(),
            "Hello World"
        );
        Ok(())
    }

    #[test]
    fn test_write_long_string_failure() -> Result<(), ErrorTypes> {
        let mut body = ProtocolBody::new();
        let large_string = "a".repeat(i32::MAX as usize + 1);
        let res = body.write_long_string(large_string);
        assert!(res.is_err());
        Ok(())
    }

    #[test]
    fn test_write_string_list() -> Result<(), ErrorTypes> {
        let mut body = ProtocolBody::new();
        body.write_string_list(vec![String::from("Hello"), String::from("World")])?;
        let n = u16::from_be_bytes(body.data[0..2].try_into().unwrap());
        assert_eq!(n, 2);
        let n = u16::from_be_bytes(body.data[2..4].try_into().unwrap());
        assert_eq!(n, 5);
        assert_eq!(
            String::from_utf8(body.data[4..9].to_vec()).unwrap(),
            "Hello"
        );
        let n = u16::from_be_bytes(body.data[9..11].try_into().unwrap());
        assert_eq!(n, 5);
        assert_eq!(
            String::from_utf8(body.data[11..16].to_vec()).unwrap(),
            "World"
        );
        Ok(())
    }

    #[test]
    fn test_write_string_list_failure() -> Result<(), ()> {
        let mut body = ProtocolBody::new();
        let large_list = vec![String::from("Hello"); u16::MAX as usize + 1];
        let res = body.write_string_list(large_list);
        assert!(res.is_err());
        Ok(())
    }

    #[test]
    fn test_write_bytes() -> Result<(), ErrorTypes> {
        let mut body = ProtocolBody::new();
        body.write_bytes(vec![0x01, 0x02, 0x03], 3)?;
        let n = i32::from_be_bytes(body.data[0..4].try_into().unwrap());
        assert_eq!(n, 3);
        assert_eq!(body.data[4], 0x01);
        assert_eq!(body.data[5], 0x02);
        assert_eq!(body.data[6], 0x03);
        Ok(())
    }

    #[test]
    fn test_write_bytes_failure() -> Result<(), ()> {
        let mut body = ProtocolBody::new();
        let large_list = vec![0x01; i32::MAX as usize + 1];
        let res = body.write_bytes(large_list, 3);
        assert!(res.is_err());
        Ok(())
    }

    #[test]
    fn test_write_value() -> Result<(), ErrorTypes> {
        let mut body = ProtocolBody::new();
        body.write_value(vec![0x01, 0x02, 0x03], 3)?;
        let n = i32::from_be_bytes(body.data[0..4].try_into().unwrap());
        assert_eq!(n, 3);
        assert_eq!(body.data[4], 0x01);
        assert_eq!(body.data[5], 0x02);
        assert_eq!(body.data[6], 0x03);
        Ok(())
    }

    #[test]
    fn test_write_value_failure() -> Result<(), ()> {
        let mut body = ProtocolBody::new();
        let large_list = vec![0x01; i32::MAX as usize + 1];
        let res = body.write_value(large_list, 3);
        assert!(res.is_err());
        Ok(())
    }

    #[test]
    fn test_write_short_bytes() -> Result<(), ErrorTypes> {
        let mut body = ProtocolBody::new();
        body.write_short_bytes(vec![0x01, 0x02, 0x03])?;
        let n = u16::from_be_bytes(body.data[0..2].try_into().unwrap());
        assert_eq!(n, 3);
        assert_eq!(body.data[2], 0x01);
        assert_eq!(body.data[3], 0x02);
        assert_eq!(body.data[4], 0x03);
        Ok(())
    }

    #[test]
    fn test_write_short_bytes_failure() -> Result<(), ()> {
        let mut body = ProtocolBody::new();
        let large_list = vec![0x01; u16::MAX as usize + 1];
        let res = body.write_short_bytes(large_list);
        assert!(res.is_err());
        Ok(())
    }

    #[test]
    fn test_write_inet() -> Result<(), ErrorTypes> {
        let mut body = ProtocolBody::new();
        body.write_inet(vec![192, 168, 0, 1], 9042)?;
        assert_eq!(body.data[0], 4);
        assert_eq!(body.data[1], 192);
        assert_eq!(body.data[2], 168);
        assert_eq!(body.data[3], 0);
        assert_eq!(body.data[4], 1);
        assert_eq!(
            i32::from_be_bytes(body.data[5..9].try_into().unwrap()),
            9042
        );
        Ok(())
    }

    #[test]
    fn test_write_inet_failure() -> Result<(), ()> {
        let mut body = ProtocolBody::new();
        let large_list = vec![0x01; 5];
        let res = body.write_inet(large_list, 9042);
        assert!(res.is_err());
        Ok(())
    }

    #[test]
    fn test_write_inetaddr() -> Result<(), ErrorTypes> {
        let mut body = ProtocolBody::new();
        body.write_inetaddr(vec![192, 168, 0, 1])?;
        assert_eq!(body.data[0], 4);
        assert_eq!(body.data[1], 192);
        assert_eq!(body.data[2], 168);
        assert_eq!(body.data[3], 0);
        assert_eq!(body.data[4], 1);
        Ok(())
    }

    #[test]
    fn test_write_consistency() {
        let mut body = ProtocolBody::new();
        body.write_consistency(Consistency::One);
        assert_eq!(u16::from_be_bytes(body.data[0..2].try_into().unwrap()), 1);
    }

    #[test]
    fn test_write_string_map() -> Result<(), ErrorTypes> {
        let mut body = ProtocolBody::new();
        body.write_string_map(vec![(String::from("Hello"), String::from("World"))])?;
        let n = u16::from_be_bytes(body.data[0..2].try_into().unwrap());
        assert_eq!(n, 1);
        let n = u16::from_be_bytes(body.data[2..4].try_into().unwrap());
        assert_eq!(n, 5);
        assert_eq!(
            String::from_utf8(body.data[4..9].to_vec()).unwrap(),
            "Hello"
        );
        let n = u16::from_be_bytes(body.data[9..11].try_into().unwrap());
        assert_eq!(n, 5);
        assert_eq!(
            String::from_utf8(body.data[11..16].to_vec()).unwrap(),
            "World"
        );
        Ok(())
    }

    #[test]
    fn test_write_string_multimap() {
        let mut body = ProtocolBody::new();
        body.write_string_multimap(vec![(String::from("Hello"), vec![String::from("World")])])
            .unwrap();
        let n = u16::from_be_bytes(body.data[0..2].try_into().unwrap());
        assert_eq!(n, 1);
        let n = u16::from_be_bytes(body.data[2..4].try_into().unwrap());
        assert_eq!(n, 5);
        assert_eq!(
            String::from_utf8(body.data[4..9].to_vec()).unwrap(),
            "Hello"
        );
        let n = u16::from_be_bytes(body.data[9..11].try_into().unwrap());
        assert_eq!(n, 1);
        let n = u16::from_be_bytes(body.data[11..13].try_into().unwrap());
        assert_eq!(n, 5);
        assert_eq!(
            String::from_utf8(body.data[13..18].to_vec()).unwrap(),
            "World"
        );
    }
}
