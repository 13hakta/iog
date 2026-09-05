/// Packet header size in bytes.
pub const PACKET_HEADER_SIZE: usize = 16;

/// Protocol version (major).
pub const PROTOCOL_VERSION_MAJOR: u32 = 0;
/// Protocol version (minor).
pub const PROTOCOL_VERSION_MINOR: u32 = 2;
/// Protocol version (patch).
pub const PROTOCOL_VERSION_PATCH: u32 = 0;

/// Flag: packet data is compressed (zstd).
pub const FLAG_COMPRESSED: u8 = 0x01;

/// Client disconnect reasons.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DisconnectReason {
    /// Authentication failure.
    AuthFailed = 0,
    /// Incompatible protocol version.
    VersionMismatch = 1,
    /// Server overloaded.
    ServerFull = 2,
    /// Client is banned.
    Banned = 3,
    /// Connection limit exceeded.
    RateLimited = 4,
    /// Session not found (for Session Migration).
    SessionNotFound = 5,
}

impl From<u8> for DisconnectReason {
    fn from(value: u8) -> Self {
        match value {
            0 => DisconnectReason::AuthFailed,
            1 => DisconnectReason::VersionMismatch,
            2 => DisconnectReason::ServerFull,
            3 => DisconnectReason::Banned,
            4 => DisconnectReason::RateLimited,
            5 => DisconnectReason::SessionNotFound,
            _ => DisconnectReason::AuthFailed,
        }
    }
}

/// Packet types of the gateway exchange protocol.
///
/// Packets are divided into two categories:
/// - **Common** — used by both client and server (connection management,
///   data, keep-alive).
/// - **Extended** — used by only one side (authentication).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum PacketType {
    // ===== Common packet types (client + server) =====
    /// Packet carrying application data.
    Regular = 0,
    /// Keep-alive request (client -> server, periodic).
    Ping = 1,
    /// Keep-alive reply (server -> client).
    Pong = 2,
    /// Request to open a new logical connection.
    Open = 6,
    /// Connection open confirmation.
    Opened = 7,
    /// Connection open refusal.
    OpenFail = 8,
    /// Request to close a connection.
    Close = 9,
    /// Connection close confirmation.
    Closed = 10,

    // ===== Extended client types =====
    /// Client authentication at the server (sent by the client only).
    GwAuth = 3,

    // ===== Extended server types =====
    /// Successful authentication confirmation (sent by the server only).
    GwConnected = 4,
    /// Authentication refusal (sent by the server only).
    GwDeny = 5,

    // ===== v0.2: Protocol version =====
    /// Protocol version request (client -> server).
    ProtocolVersion = 11,

    // ===== v0.3: Challenge-Response authentication =====
    /// Challenge from the server (server -> client).
    GwAuthChallenge = 12,
    /// Response from the client (client -> server).
    GwAuthResponse = 13,

    // ===== v1.0: Session Migration =====
    /// Session restore request (client -> server).
    SessionRestore = 30,
}

impl PacketType {
    /// Returns `true` if the packet type is common to both sides.
    // Used in tests and as an API helper for packet validation.
    #[allow(dead_code)]
    pub fn is_common(&self) -> bool {
        matches!(
            self,
            PacketType::Regular
                | PacketType::Ping
                | PacketType::Pong
                | PacketType::Open
                | PacketType::Opened
                | PacketType::OpenFail
                | PacketType::Close
                | PacketType::Closed
                | PacketType::ProtocolVersion
                | PacketType::GwAuthChallenge
                | PacketType::GwAuthResponse
                | PacketType::SessionRestore
        )
    }
}

impl From<u8> for PacketType {
    fn from(value: u8) -> Self {
        match value {
            0 => PacketType::Regular,
            1 => PacketType::Ping,
            2 => PacketType::Pong,
            3 => PacketType::GwAuth,
            4 => PacketType::GwConnected,
            5 => PacketType::GwDeny,
            6 => PacketType::Open,
            7 => PacketType::Opened,
            8 => PacketType::OpenFail,
            9 => PacketType::Close,
            10 => PacketType::Closed,
            11 => PacketType::ProtocolVersion,
            12 => PacketType::GwAuthChallenge,
            13 => PacketType::GwAuthResponse,
            30 => PacketType::SessionRestore,
            _ => PacketType::Regular,
        }
    }
}

/// Binary packet of the gateway exchange protocol.
///
/// Header format (16 bytes, little-endian):
///
/// ```text
/// offset    size    field         description
/// --------  ------  ------------  -------------------------------
///  0        1       packet_type   packet type (PacketType)
///  1        8       packet_id     packet identifier (u64 LE)
///  9        2       link_id       connection identifier (u16 LE)
/// 11        2       data_size     size of the data following the header (u16 LE)
/// 13        1       flags         packet flags (FLAG_COMPRESSED, etc.)
/// 14        2       reserved      reserved (zeros)
/// ```
///
/// Up to `data_size` bytes of data may follow the header in a packet.
#[derive(Clone, Debug)]
pub struct PacketGW {
    pub packet_type: PacketType,
    pub packet_id: u64,
    pub link_id: u16,
    pub data_size: u16,
    /// Packet flags (FLAG_COMPRESSED, etc.).
    pub flags: u8,
}

impl PacketGW {
    /// Creates a new packet without flags.
    pub fn new(packet_type: PacketType, packet_id: u64, link_id: u16, data_size: u16) -> Self {
        Self {
            packet_type,
            packet_id,
            link_id,
            data_size,
            flags: 0,
        }
    }

    /// Checks whether the compression flag is set.
    pub fn is_compressed(&self) -> bool {
        self.flags & FLAG_COMPRESSED != 0
    }

    /// Sets flag(s) in the packet header.
    pub fn set_flag(&mut self, flag: u8) {
        self.flags |= flag;
    }
}

impl PacketGW {
    /// Serializes the packet header into a fixed-size byte array.
    pub fn to_bytes(&self) -> [u8; PACKET_HEADER_SIZE] {
        let mut bytes: [u8; PACKET_HEADER_SIZE] = [0; PACKET_HEADER_SIZE];
        bytes[0] = self.packet_type as u8;
        bytes[1..9].copy_from_slice(&self.packet_id.to_le_bytes());
        bytes[9..11].copy_from_slice(&self.link_id.to_le_bytes());
        bytes[11..13].copy_from_slice(&self.data_size.to_le_bytes());
        bytes[13] = self.flags;
        bytes
    }

    /// Restores a packet from header bytes.
    ///
    /// Returns `None` if the buffer is smaller than the header size.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < PACKET_HEADER_SIZE {
            return None;
        }

        let packet_type = PacketType::from(bytes[0]);
        let packet_id = u64::from_le_bytes(bytes[1..9].try_into().ok()?);
        let link_id = u16::from_le_bytes(bytes[9..11].try_into().ok()?);
        let data_size = u16::from_le_bytes(bytes[11..13].try_into().ok()?);
        let flags = bytes[13];

        Some(Self {
            packet_type,
            packet_id,
            link_id,
            data_size,
            flags,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_packet_roundtrip() {
        let packet = PacketGW::new(PacketType::Open, 42, 7, 1024);
        let bytes = packet.to_bytes();
        let parsed = PacketGW::from_bytes(&bytes).expect("packet should parse");

        assert_eq!(parsed.packet_type, PacketType::Open);
        assert_eq!(parsed.packet_id, 42);
        assert_eq!(parsed.link_id, 7);
        assert_eq!(parsed.data_size, 1024);
    }

    #[test]
    fn test_common_types() {
        assert!(PacketType::Regular.is_common());
        assert!(PacketType::Ping.is_common());
        assert!(PacketType::Open.is_common());
        assert!(!PacketType::GwAuth.is_common());
        assert!(!PacketType::GwConnected.is_common());
        assert!(!PacketType::GwDeny.is_common());
    }

    #[test]
    fn test_from_short_buffer() {
        assert!(PacketGW::from_bytes(&[0; 5]).is_none());
    }
}
