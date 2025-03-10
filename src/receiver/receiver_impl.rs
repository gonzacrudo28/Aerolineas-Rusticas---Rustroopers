extern crate lz4;

use super::{read_notation, response_message::ResponseMessage, result_response::ResultResponse};
use crate::errors::error_types::ErrorTypes;
use crate::protocol::frames_headers::{
    flags::Flags, header::Header, opcode::Opcode, version::Version,
};
use crate::protocol::protocol_body::compression::{self, Compression};
use crate::protocol::protocol_notations::{flags_row::FlagsRow, value::Value};
use crate::protocol::query_parser::parser_impl::parse_query;
use crate::receiver::{message::Message, request_message::RequestMessage};
use read_notation::*;

/// Parses a frame and returns the corresponding Message struct.
pub fn receive_message(
    bytes: &mut Vec<u8>,
    compression: &Option<Compression>,
) -> Result<Message, ErrorTypes> {
    if bytes.len() < 9 {
        return Err(ErrorTypes::new(311, "Invalid frame".to_string()));
    }
    let mut body_bytes: Vec<u8> = bytes.split_off(9);
    let header: Header = create_header(bytes)?;
    let body: Message = analyze_body(header, &mut body_bytes, compression.clone())?;
    Ok(body)
}

/// This function receives an array of bytes and returns a Header struct.
fn create_header(bytes: &mut [u8]) -> Result<Header, ErrorTypes> {
    let version: Version = match bytes[0] {
        0x05 => Version::Request,
        0x85 => Version::Response,
        _ => return Err(ErrorTypes::new(312, "Invalid version".to_string())),
    };
    let flag: Vec<Flags> = get_flag(bytes[1])?;
    let stream = u16::from_be_bytes([bytes[2], bytes[3]]);
    let opcode = get_opcode(bytes[4])?;
    let length = i32::from_be_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]);
    let mut header = Header::new();
    header.set_flag(flag);
    header.set_stream(stream);
    header.set_opcode(opcode);
    header.set_length(length);
    header.set_version(version);
    Ok(header)
}

/// This function receives the header and an array of bytes, decode it if its request or response.
fn analyze_body(
    header: Header,
    bytes: &mut Vec<u8>,
    compression: Option<Compression>,
) -> Result<Message, ErrorTypes> {
    if let Some(compress) = compression {
        match compression::Compression::decompression(&compress, bytes.clone()) {
            Ok(data) => {
                *bytes = data;
            }
            Err(e) => return Err(e),
        }
    }
    match header.get_version() {
        Version::Request => handle_request(header, bytes),
        Version::Response => handle_response(header, bytes),
    }
}

/// This function receives a byte and returns an Opcode.
fn get_opcode(byte: u8) -> Result<Opcode, ErrorTypes> {
    match byte {
        0x00 => Ok(Opcode::Error),
        0x01 => Ok(Opcode::StartUp),
        0x02 => Ok(Opcode::Ready),
        0x03 => Ok(Opcode::Authenticate),
        0x07 => Ok(Opcode::Query),
        0x08 => Ok(Opcode::Result),
        0x0F => Ok(Opcode::AuthResponse),
        0x10 => Ok(Opcode::AuthSuccess),
        _ => Err(ErrorTypes::new(313, "Invalid opcode".to_string())),
    }
}

/// This function receives a byte and returns a Vec of Flags.
fn get_flag(byte: u8) -> Result<Vec<Flags>, ErrorTypes> {
    let mut result: Vec<Flags> = Vec::new();
    match byte {
        0x00 => (),
        0x01 => result.push(Flags::Compression),
        0x02 => result.push(Flags::Tracing),
        0x03 => {
            result.push(Flags::Compression);
            result.push(Flags::Tracing);
        }
        0x04 => result.push(Flags::CustomPayload),
        0x05 => {
            result.push(Flags::Compression);
            result.push(Flags::CustomPayload);
        }
        0x06 => {
            result.push(Flags::CustomPayload);
            result.push(Flags::Tracing);
        }
        0x07 => {
            result.push(Flags::Compression);
            result.push(Flags::CustomPayload);
            result.push(Flags::Tracing);
        }
        0x08 => result.push(Flags::Warning),
        0x09 => {
            result.push(Flags::Compression);
            result.push(Flags::Warning);
        }
        0x0A => result.push(Flags::Beta),
        _ => return Err(ErrorTypes::new(314, "Invalid flag".to_string())),
    }
    Ok(result)
}

/// This function receives the header and an array of bytes, decode and handle the request mesit if its request or response.
fn handle_request(header: Header, bytes: &mut Vec<u8>) -> Result<Message, ErrorTypes> {
    match header.get_opcode() {
        Opcode::StartUp => handle_startup(bytes),
        Opcode::AuthResponse => handle_auth_response(bytes),
        Opcode::Query => handle_query(bytes),
        _ => Err(ErrorTypes::new(315, "Invalid opcode".to_string())),
    }
}

/// This function receives the header and the array of bytes representing the body of the message and handles it according to the opcode.
fn handle_response(header: Header, bytes: &mut Vec<u8>) -> Result<Message, ErrorTypes> {
    match header.get_opcode() {
        Opcode::Error => handle_error(bytes),
        Opcode::Ready => handle_ready(bytes),
        Opcode::Authenticate => handle_authenticate(bytes),
        Opcode::Result => handle_result(bytes),
        Opcode::AuthSuccess => handle_auth_success(bytes),
        _ => Err(ErrorTypes::new(316, "Invalid opcode".to_string())),
    }
}

/// This function handle the startup message.
fn handle_startup(bytes: &mut Vec<u8>) -> Result<Message, ErrorTypes> {
    let options = read_string_map(bytes)?;
    match options.get("CQL_VERSION") {
        Some(version) => {
            if version != "3.0.0" {
                return Err(ErrorTypes::new(317, "Invalid CQL version".to_string()));
            }
        }
        None => return Err(ErrorTypes::new(318, "CQL version not found".to_string())),
    }
    if let Some(compression) = options.get("COMPRESSION") {
        if compression == "snappy" {
            return Ok(Message::SolicitationMessage(RequestMessage::StartUp {
                compression: Some(Compression::Snappy),
            }));
        } else if compression == "lz4" {
            return Ok(Message::SolicitationMessage(RequestMessage::StartUp {
                compression: Some(Compression::LZ4),
            }));
        }
        return Err(ErrorTypes::new(
            319,
            "Invalid compression algorithm".to_string(),
        ));
    }
    Ok(Message::SolicitationMessage(RequestMessage::StartUp {
        compression: None,
    }))
}

/// This function handle the auth response message.
fn handle_auth_response(bytes: &mut Vec<u8>) -> Result<Message, ErrorTypes> {
    let (_, vec) = read_bytes(bytes)?;
    match vec {
        Value::Normal(bytes) => {
            let token = match String::from_utf8(bytes) {
                Ok(token) => token,
                _ => return Err(ErrorTypes::new(327, "Invalid auth response".to_string())),
            };
            let user_password = token.split(",").collect::<Vec<&str>>();
            Ok(Message::SolicitationMessage(RequestMessage::AuthResponse {
                auth_response: (user_password[0].to_string(), user_password[1].to_string()),
            }))
        }
        _ => Err(ErrorTypes::new(320, "Invalid auth response".to_string())),
    }
}

/// This function handle the auth success message.
fn handle_auth_success(bytes: &mut [u8]) -> Result<Message, ErrorTypes> {
    if !bytes.is_empty() {
        return Err(ErrorTypes::new(321, "Invalid auth response".to_string()));
    }
    Ok(Message::ReplyMessage(ResponseMessage::AuthSuccess {
        body: "".to_string(),
    }))
}

/// This function handle the error message.
fn handle_error(bytes: &mut Vec<u8>) -> Result<Message, ErrorTypes> {
    let code: i32 = read_int(bytes)?;
    let message: String = read_string(bytes)?;
    Ok(Message::ReplyMessage(ResponseMessage::Error {
        code,
        message,
    }))
}

/// This function handle the ready message.
fn handle_ready(bytes: &mut [u8]) -> Result<Message, ErrorTypes> {
    if !bytes.is_empty() {
        return Err(ErrorTypes::new(321, "Invalid body".to_string()));
    }
    Ok(Message::ReplyMessage(ResponseMessage::Ready {
        body: "".to_string(),
    }))
}

/// This function handle the authenticate message.
fn handle_authenticate(bytes: &mut Vec<u8>) -> Result<Message, ErrorTypes> {
    let class: String = read_string(bytes)?;
    Ok(Message::ReplyMessage(ResponseMessage::Authenticate {
        class,
    }))
}

/// This function handle the result message.
fn handle_result(bytes: &mut Vec<u8>) -> Result<Message, ErrorTypes> {
    let kind = read_int(bytes)?;
    let message = match kind {
        1 => handle_void_result(bytes)?,
        2 => handle_rows_result(bytes)?,
        5 => handle_schema_change_result(bytes)?,
        3 => handle_set_keyspace_result(bytes)?,
        _ => return Err(ErrorTypes::new(322, "Invalid result kind".to_string())),
    };
    Ok(message)
}

/// This function handle the void result.
fn handle_void_result(bytes: &mut [u8]) -> Result<Message, ErrorTypes> {
    if !bytes.is_empty() {
        return Err(ErrorTypes::new(323, "Invalid body".to_string()));
    }
    Ok(Message::ReplyMessage(ResponseMessage::Result {
        kind: ResultResponse::Void,
    }))
}

/// This function handle the set keyspace result.
fn handle_set_keyspace_result(bytes: &mut Vec<u8>) -> Result<Message, ErrorTypes> {
    let keyspace = read_string(bytes)?;
    Ok(Message::ReplyMessage(ResponseMessage::Result {
        kind: ResultResponse::SetKeyspace { keyspace },
    }))
}

/// This function handle the schema change result.
fn handle_schema_change_result(bytes: &mut Vec<u8>) -> Result<Message, ErrorTypes> {
    if !bytes.is_empty() {
        let change_type = read_string(bytes)?;
        let target = read_string(bytes)?;
        let options = read_string(bytes)?;
        Ok(Message::ReplyMessage(ResponseMessage::Result {
            kind: ResultResponse::SchemaChange {
                change_type: change_type.to_string(),
                target: target.to_string(),
                options: options.to_string(),
            },
        }))
    } else {
        Err(ErrorTypes::new(324, "Invalid body".to_string()))
    }
}

/// This function returns the rows result.
fn handle_rows_result(bytes: &mut Vec<u8>) -> Result<Message, ErrorTypes> {
    let flags = match read_int(bytes)? {
        0x0002 => FlagsRow::HasMorePages,
        0x0004 => FlagsRow::NoMetadata,
        _ => return Err(ErrorTypes::new(325, "Invalid flags".to_string())),
    };

    let column_count = read_int(bytes)?;
    let row_count = read_int(bytes)?;
    let mut row: Vec<String> = Vec::new();
    let mut rows: Vec<Vec<String>> = Vec::new();

    for _ in 0..row_count {
        for _ in 0..column_count {
            let (_, value) = read_bytes(bytes)?;
            match value {
                Value::Normal(bytes) => {
                    row.push(String::from_utf8(bytes).unwrap());
                }
                Value::Null => {
                    row.push("".to_string());
                }
                _ => return Err(ErrorTypes::new(326, "Invalid value".to_string())),
            }
        }
        rows.push(row);
        row = Vec::new();
    }

    Ok(Message::ReplyMessage(ResponseMessage::Result {
        kind: ResultResponse::Rows {
            metadata: flags,
            rows,
        },
    }))
}

/// This functions receives the query and parse it.
fn handle_query(bytes: &mut Vec<u8>) -> Result<Message, ErrorTypes> {
    let query = read_long_string(bytes)?;
    let consistency = read_consistency(bytes)?;
    match parse_query(query.clone()) {
        Ok(parsed_query) => Ok(Message::SolicitationMessage(RequestMessage::Query(
            parsed_query,
            consistency,
            query,
        ))),
        Err(e) => Err(e),
    }
}
