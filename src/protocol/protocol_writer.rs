use std::vec;

use super::{
    frames_headers::{flags, header::Header, opcode::Opcode, version::Version},
    protocol_body::{
        compression::Compression, query_flags::QueryFlags, result_kind::ResultKind,
        schema_change::SchemaChangeType,
    },
    protocol_notations::{consistency, protocol_body_writer::ProtocolBody},
};

use crate::{
    errors::error_types::ErrorTypes, protocol::frames_headers::flags::Flags,
    protocol::protocol_notations::flags_row::FlagsRow,
};

pub struct Protocol {
    header: Header,
    body: ProtocolBody,
    compression: Option<Compression>,
    length: i32,
}

impl Default for Protocol {
    fn default() -> Self {
        Self::new()
    }
}

impl Protocol {
    pub fn new() -> Protocol {
        Protocol {
            header: Header::new(),
            body: ProtocolBody::new(),
            compression: None,
            length: 0,
        }
    }
    pub fn get_header(&self) -> &Header {
        &self.header
    }

    pub fn get_body(&self) -> &ProtocolBody {
        &self.body
    }

    pub fn get_length(&self) -> i32 {
        self.length
    }

    pub fn set_compress_algorithm(&mut self, compression: Option<Compression>) {
        self.compression = compression;
    }
    pub fn get_binary(&mut self) -> Vec<u8> {
        let mut binary: Vec<u8> = Vec::new();
        binary.append(&mut self.header.get_binary());
        self.length = binary.len() as i32;
        match &self.compression {
            Some(compres) => {
                let mut body = self.body.get_binary();
                let len = body.len();

                if !body.is_empty() {
                    let mut compressed = compres.compression(body).unwrap();
                    let bytes: [u8; 4] = len.to_be_bytes()[4..].try_into().unwrap();
                    binary.extend_from_slice(&bytes);
                    binary.append(&mut compressed);
                } else {
                    binary.append(&mut body);
                }
            }
            None => {
                binary.append(&mut self.body.get_binary());
            }
        }
        binary
    }

    /// This function writes the StartUp message
    pub fn write_startup(&mut self, compression: Option<Compression>) -> Result<(), ErrorTypes> {
        self.header.set_version(Version::Request);
        self.header.set_opcode(Opcode::StartUp);
        let mut vec = vec![(String::from("CQL_VERSION"), String::from("3.0.0"))];
        if let Some(c) = compression {
            let compression = match c {
                Compression::Snappy => "snappy",
                Compression::LZ4 => "lz4",
            };
            vec.push((String::from("COMPRESSION"), String::from(compression)));
            self.header.set_flag(vec![flags::Flags::Compression]);
        };
        self.body.write_string_map(vec)?;
        self.header.set_length(self.body.get_length() as i32);
        Ok(())
    }

    /// This function writes the Auth_Response message
    pub fn write_auth_response(&mut self, user: (String, String)) -> Result<(), ErrorTypes> {
        self.header.set_version(Version::Request);
        self.header.set_opcode(Opcode::AuthResponse);
        self.set_compression();
        let mut user_password = user.0.into_bytes();
        user_password.push(b',');
        user_password.extend_from_slice(user.1.as_bytes());
        let len = user_password.len() as i32;
        self.body.write_bytes(user_password, len)?;
        self.header.set_length(self.body.get_length() as i32);
        Ok(())
    }

    // Query
    /// This function writes the body of a Query message
    pub fn write_query(
        &mut self,
        query: &str,
        consistency: consistency::Consistency,
        flags: Vec<QueryFlags>,
    ) -> Result<(), ErrorTypes> {
        self.header.set_version(Version::Request);
        self.set_compression();
        self.header.set_opcode(Opcode::Query);
        self.body.write_long_string(query.to_string())?;
        self.body.write_consistency(consistency);
        self.body.write_byte(flags.iter().map(|x| *x as u8).sum());

        for flag in flags.iter() {
            write_flag(
                &mut self.body,
                flag,
                Some(vec![QueryFlags::SkipMetadata as u8]),
            )?;
        }
        self.header.set_length(self.body.get_length() as i32);
        Ok(())
    }

    //RESPONSES
    /// This function writes the body of an Error message
    pub fn write_error(&mut self, code: i32, message: &str) -> Result<(), ErrorTypes> {
        self.header.set_version(Version::Response);
        self.header.set_flag(vec![]);
        self.header.set_opcode(Opcode::Error);
        self.body.write_int(code);
        self.body.write_string(message.to_string())?;
        self.header.set_length(self.body.get_length() as i32);
        Ok(())
    }

    /// This function writes the body of Ready message
    pub fn write_ready(&mut self) {
        self.header.set_version(Version::Response);
        self.header.set_flag(vec![]);
        self.header.set_opcode(Opcode::Ready);
        self.header.set_length(self.body.get_length() as i32);
    }

    /// This function writes the body of an Authenticate message
    pub fn write_authenticate(&mut self, authenticator: &str) -> Result<(), ErrorTypes> {
        self.header.set_version(Version::Response);
        self.header.set_opcode(Opcode::Authenticate);
        self.body.write_string(authenticator.to_string())?;
        self.header.set_length(self.body.get_length() as i32);
        Ok(())
    }

    /// This function writes the body of an AuthSuccess message
    pub fn write_auth_success(&mut self) {
        self.header.set_version(Version::Response);
        self.set_compression();
        self.header.set_opcode(Opcode::AuthSuccess);
        self.header.set_length(self.body.get_length() as i32);
    }

    /// This function writes the body of a Result message
    pub fn write_result(
        &mut self,
        result_kind: ResultKind,
        values: Option<Vec<Vec<String>>>,
        keyspace: Option<&str>,
        schema_change: Option<SchemaChangeType>,
        target: Option<String>,
        options: Option<&String>,
    ) {
        //In values we have the possible body dependig on the ResultKind
        self.header.set_version(Version::Response);
        self.set_compression();
        self.header.set_opcode(Opcode::Result);
        self.body.write_int(result_kind as i32);
        write_result_kind(
            &mut self.body,
            result_kind,
            values,
            keyspace,
            schema_change,
            target,
            options,
        )
        .unwrap();
        self.header.set_length(self.body.get_length() as i32);
    }

    fn set_compression(&mut self) {
        if self.compression.is_some() {
            self.header.set_flag(vec![Flags::Compression]);
        }
    }
}

/// This private function writes the kind of a Result message
fn write_result_kind(
    body: &mut ProtocolBody,
    result_kind: ResultKind,
    values: Option<Vec<Vec<String>>>,
    keyspace: Option<&str>,
    schema_change: Option<SchemaChangeType>,
    schema_change_target: Option<String>,
    schema_change_options: Option<&String>,
) -> Result<(), ErrorTypes> {
    if let ResultKind::Rows = result_kind {
        let values = match values {
            Some(v) => v,
            None => {
                return Err(ErrorTypes::new(
                    404,
                    "Error in values while writing result".to_string(),
                ))
            }
        };
        body.write_int(FlagsRow::NoMetadata as i32);
        let len = values[0].len() as i32;
        body.write_int(len);
        body.write_int(values.len() as i32);
        for row in values.iter() {
            for column in row.iter() {
                let column = column.as_bytes();
                body.write_bytes(column.to_vec(), column.len() as i32)?;
            }
        }
    } else if let ResultKind::SetKeyspace = result_kind {
        body.write_string(keyspace.unwrap().to_string())?
    } else if let ResultKind::SchemaChange = result_kind {
        match schema_change {
            Some(change_type) => {
                let change_type = match change_type {
                    SchemaChangeType::Created => "CREATED",
                    SchemaChangeType::Updated => "UPDATED",
                    SchemaChangeType::Dropped => "DROPPED",
                };
                body.write_string(change_type.to_string())?;
            }
            None => {
                return Err(ErrorTypes::new(405, "Error writing result".to_string()));
            }
        }
        match schema_change_target {
            Some(target) => body.write_string(target)?,
            None => {
                return Err(ErrorTypes::new(406, "Error writing result".to_string()));
            }
        }
        match schema_change_options {
            Some(options) => body.write_string(options.to_string())?,
            None => {
                return Err(ErrorTypes::new(407, "Error writing result".to_string()));
            }
        }
    } else if let ResultKind::Void = result_kind {
        //No body
    } else {
        return Err(ErrorTypes::new(408, "Error writing result".to_string()));
    }
    Ok(())
}

/// This private function writes the flags of a Query message
fn write_flag(
    body: &mut ProtocolBody,
    flag: &QueryFlags,
    _values: Option<Vec<u8>>,
) -> Result<(), ErrorTypes> {
    if let QueryFlags::SkipMetadata = flag {
        body.write_byte(QueryFlags::SkipMetadata as u8)
    }
    Ok(())
}

#[cfg(test)]
pub mod test {
    use super::*;

    #[test]
    fn test_write_startup() {
        let protocol = Protocol::new();
        protocol.get_header().get_version();
        assert_eq!(protocol.get_header().get_length(), 0);
    }

    #[test]
    fn test_write_query() {
        let mut protocol = Protocol::new();
        let res = protocol.write_query(
            "SELECT * FROM users",
            consistency::Consistency::One,
            vec![QueryFlags::SkipMetadata],
        );
        assert_eq!(res, Ok(()));
    }
}
