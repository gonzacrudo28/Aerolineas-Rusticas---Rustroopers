use super::{flags::Flags, opcode::Opcode, version::Version};

#[derive(Debug)]
/// Represents the header of a frame.
///
/// The header is a critical component of a frame, containing metadata about the frame's
/// protocol version, flags, stream ID, operation type, and payload length. This information
/// is essential for interpreting the frame's contents and handling it appropriately.
///
/// ### Fields:
/// - **version**: Specifies the protocol version used in the frame.
/// - **flag**: A list of `Flags` that modify the behavior or provide additional metadata for the frame.
/// - **stream**: The stream ID (`u16`) that uniquely identifies the frame within a connection.
/// - **opcode**: The `Opcode` specifying the type of operation the frame represents (e.g., query, response).
/// - **length**: The length (`i32`) of the frame's body in bytes.e.
pub struct Header {
    version: Version,
    flag: Vec<Flags>,
    stream: u16,
    opcode: Opcode,
    length: i32,
}

impl Default for Header {
    fn default() -> Self {
        Self::new()
    }
}

impl Header {
    /// Provides a default implementation for `Header`.
    pub fn new() -> Header {
        Header {
            version: Version::Request,
            flag: Vec::new(),
            stream: 0,
            opcode: Opcode::Error,
            length: 0,
        }
    }

    /// Retrieves the version of the `Header`.
    pub fn get_version(&self) -> Version {
        self.version
    }

    /// Retrieves the flags set in the `Header`.
    pub fn get_flag(&self) -> &Vec<Flags> {
        &self.flag
    }

    /// Retrieves the stream id of the `Header`.
    pub fn get_stream(&self) -> u16 {
        self.stream
    }

    /// Retrieves the operation code of the `Header`.
    pub fn get_opcode(&self) -> Opcode {
        self.opcode
    }

    /// Retrieves the length of the frame's body.
    pub fn get_length(&self) -> i32 {
        self.length
    }

    /// Sets the length of the frame's body.
    ///
    /// # Arguments:
    /// - `length`: The new length value.
    pub fn set_length(&mut self, length: i32) {
        self.length = length;
    }

    /// Sets the version of the header.
    ///
    /// # Arguments:
    /// - `version`: The new version value.
    pub fn set_version(&mut self, version: Version) {
        self.version = version;
    }

    /// Sets the operation code of the header.
    ///
    /// # Arguments:
    /// - `opcode`: The new operation code.
    pub fn set_opcode(&mut self, opcode: Opcode) {
        self.opcode = opcode;
    }

    /// Sets the stream ID of the header.
    ///
    /// # Arguments:
    /// - `stream`: The new stream ID.
    pub fn set_stream(&mut self, stream: u16) {
        self.stream = stream;
    }

    /// Sets the flags for the header.
    ///
    /// # Arguments:
    /// - `flag`: A vector of `Flags` to set.
    pub fn set_flag(&mut self, flag: Vec<Flags>) {
        self.flag = flag;
    }

    /// Converts the header into its binary representation. # Returns: A `Vec<u8>` containing the serialized header. # Details: - The binary representation includes the version, combined flags, stream ID, opcode, and length.
    pub fn get_binary(&self) -> Vec<u8> {
        let mut bits_res: Vec<u8> = Vec::new();
        bits_res.push(self.version as u8);
        bits_res.push(self.flag.iter().map(|x| *x as u8).sum());
        bits_res.extend(self.stream.to_be_bytes());
        bits_res.push(self.opcode as u8);
        bits_res.extend(self.length.to_be_bytes());
        bits_res
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_header() {
        let mut header = Header::new();
        header.set_version(Version::Request);
        header.set_flag(vec![Flags::Compression, Flags::Tracing]);
        header.set_stream(0x0001);
        header.set_opcode(Opcode::StartUp);

        assert_eq!(header.get_version(), Version::Request);
        assert_eq!(header.get_flag(), &[Flags::Compression, Flags::Tracing]);
        assert_eq!(header.get_stream(), 0x0001);
        assert_eq!(header.get_opcode(), Opcode::StartUp);
        assert_eq!(
            header.get_binary(),
            vec![0x05, 0x03, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00]
        );
    }
}
