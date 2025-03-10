use serde::{Deserialize, Serialize};
extern crate snap;
use snap::read::FrameDecoder;
use snap::write::FrameEncoder;
use std::io::{Read, Write};
extern crate lz4;
use crate::errors::error_types::ErrorTypes;
use lz4::block::{compress, decompress};

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Represents the different compression algorithms supported by the protocol.
///
/// Compression algorithms are used to reduce the size of data transmitted in messages,
/// improving performance and reducing network load.
pub enum Compression {
    Snappy,
    LZ4,
}

impl Compression {
    /// Compresses the given data using the selected compression algorithm.
    ///
    /// This function applies the compression algorithm associated with the `Compression` instance
    /// to the provided data. If the compression process fails, an `ErrorTypes` instance is returned
    /// with details about the error.
    ///
    /// ### Parameters:
    /// - `data` (`Vec<u8>`): The data to be compressed.
    ///
    /// ### Returns:
    /// - `Ok(Vec<u8>)`: The compressed data as a vector of bytes if compression succeeds.
    /// - `Err(ErrorTypes)`: An error indicating the failure of the compression process.
    pub fn compression(&self, data: Vec<u8>) -> Result<Vec<u8>, ErrorTypes> {
        match self {
            Compression::Snappy => {
                let mut encoder = FrameEncoder::new(Vec::new());
                encoder.write_all(&data).map_err(|_| {
                    ErrorTypes::new(410, "Error compressing data by Snappy".to_string())
                })?;
                let compressed = encoder.into_inner().map_err(|_| {
                    ErrorTypes::new(410, "Error compressing data by Snappy".to_string())
                })?;
                Ok(compressed)
            }
            Compression::LZ4 => compress(&data, None, false)
                .map_err(|_| ErrorTypes::new(411, "Error compressing data by LZ4".to_string())),
        }
    }

    /// Decompresses the given data using the selected compression algorithm.
    ///
    /// This function applies the decompression algorithm associated with the `Compression` instance
    /// to the provided data. If the decompression process fails, an `ErrorTypes` instance is returned
    /// with details about the error.
    ///
    /// ### Parameters:
    /// - `data` (`Vec<u8>`): The compressed data to be decompressed.
    ///
    /// ### Returns:
    /// - `Ok(Vec<u8>)`: The decompressed data as a vector of bytes if decompression succeeds.
    /// - `Err(ErrorTypes)`: An error indicating the failure of the decompression process.    
    pub fn decompression(&self, data: Vec<u8>) -> Result<Vec<u8>, ErrorTypes> {
        match self {
            Compression::Snappy => {
                let mut decoder = FrameDecoder::new(&data[4..]);
                let mut decompressed = Vec::new();
                decoder
                    .read_to_end(&mut decompressed)
                    .map_err(|_| ErrorTypes::new(411, "Error decompressing data".to_string()))?;
                Ok(decompressed)
            }
            Compression::LZ4 => {
                let len: [u8; 4] = data[..4].try_into().map_err(|_| {
                    ErrorTypes::new(411, "Error decompressing data lz4".to_string())
                })?;
                decompress(&data[4..], Some(i32::from_be_bytes(len)))
                    .map_err(|_| ErrorTypes::new(411, "Error decompressing data lz4".to_string()))
            }
        }
    }
}
