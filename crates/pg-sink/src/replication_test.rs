use super::*;

#[test]
fn lsn_formats_as_x_slash_y() {
    assert_eq!(lsn_xy(Lsn::ZERO), "0/0");
    assert_eq!(lsn_xy(Lsn::new(0x1_9A2B_3C4D)), "1/9A2B3C4D");
    assert_eq!(lsn_xy(Lsn::new(0x0199_BAC8)), "0/199BAC8");
}

#[test]
fn standby_status_frame_layout() {
    let s = StandbyStatus {
        write: Lsn::new(0x100),
        flush: Lsn::new(0x80),
        apply: Lsn::new(0x40),
        reply_requested: true,
    };
    let msg = build_standby_status(s);
    assert_eq!(msg[0], b'd'); // CopyData
    let len = u32::from_be_bytes([msg[1], msg[2], msg[3], msg[4]]) as usize;
    assert_eq!(len, msg.len() - 1); // length is self-inclusive, excludes the tag
    assert_eq!(msg[5], b'r'); // standby status update
    assert_eq!(read_lsn(&msg[6..14]).as_u64(), 0x100); // write
    assert_eq!(read_lsn(&msg[14..22]).as_u64(), 0x80); // flush
    assert_eq!(read_lsn(&msg[22..30]).as_u64(), 0x40); // apply
    assert_eq!(*msg.last().unwrap(), 1); // reply_requested
}

#[test]
fn take_message_needs_a_full_frame() {
    let mut buf = BytesMut::new();
    buf.extend_from_slice(b"Z"); // tag only
    buf.extend_from_slice(&5u32.to_be_bytes()); // length = 5 (4 + 1 body byte)
    assert!(
        take_message(&mut buf).is_none(),
        "body byte not yet present"
    );
    buf.extend_from_slice(b"I"); // the 1 body byte (idle)
    let (tag, body) = take_message(&mut buf).unwrap();
    assert_eq!(tag, b'Z');
    assert_eq!(&body[..], b"I");
    assert!(buf.is_empty());
}

#[test]
fn error_message_extracts_the_message_field() {
    // Fields: S<severity>\0 C<code>\0 M<message>\0 \0
    let body = b"SERROR\0C42704\0Mno such slot\0\0";
    assert_eq!(error_message(body), "no such slot");
}
