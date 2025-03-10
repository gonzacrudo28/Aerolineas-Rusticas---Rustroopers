#[derive(Debug, Clone, Copy, PartialEq)]
/// Represents the flags that can be set in the header of a frame. These flags are used to modify the behavior or provide additional metadata for the frame. Each flag corresponds to a specific feature or characteristic, and they can be combined using bitwise operations.
pub enum Flags {
    Compression = 0x01,
    Tracing = 0x02,
    CustomPayload = 0x04,
    Warning = 0x08,
    Beta = 0x10,
}
