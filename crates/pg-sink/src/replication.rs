//! The hand-rolled logical-replication consumer (§1.2 — "we own the connection … don't adopt a
//! framework") and its standby-status keepalive feedback (§1.9).
//!
//! **Spike outcome (the pivot point):** `tokio-postgres` 0.7 has **no** replication surface — no way
//! to open a `replication=database` connection, no CopyBoth duplex. Rather than adopt
//! `pgwire-replication`, we hand-roll the wire protocol over a raw `TcpStream`, exactly as §1.2
//! prescribes: a Startup handshake, `START_REPLICATION`, then the CopyBoth byte stream (`'w'`
//! XLogData / `'k'` primary keepalive), replying with `'r'` standby-status updates. The dev harness
//! uses `trust` auth so the handshake carries no SCRAM (SCRAM would be added here if a
//! password-authed source were required). The `ReplicationStream` / `ReplicationMessage` /
//! `StandbyStatus` seam is unchanged for callers, so PR 2.21's decoder plugs in regardless.
//!
//! **Two LSNs, kept apart (§1.9):** the *received* LSN (sent as `write` to stay connected) advances
//! here on every frame; `flush`/`apply` (= `confirmed_flush_lsn`, which releases source WAL) only
//! advance on durability — PR 2.26 — so we hold them at the durable baseline. Keepalive feedback is
//! **unconditional**: it goes out well under `wal_sender_timeout`, never gated on S3 durability, or
//! the walsender severs us with a reconnect storm.

use anyhow::{Context, anyhow, bail};
use bytes::{Bytes, BytesMut};
use common::{Lsn, PG_EPOCH_UNIX_SECS};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::Instant;

/// Default feedback cadence: well under any sane `wal_sender_timeout` (the dev harness uses 5s).
const DEFAULT_FEEDBACK_INTERVAL: Duration = Duration::from_secs(1);

/// One CopyBoth frame off the wire. The XLogData payload stays opaque `Bytes` — PR 2.21's pgoutput
/// decoder consumes it directly (zero-copy).
#[derive(Debug)]
pub enum ReplicationMessage {
    /// `'w'` — XLogData.
    XLogData {
        wal_start: Lsn,
        wal_end: Lsn,
        server_clock: i64,
        data: Bytes,
    },
    /// `'k'` — primary keepalive.
    Keepalive {
        wal_end: Lsn,
        server_clock: i64,
        reply_requested: bool,
    },
}

/// A `'r'` standby status update. **`write ≥ flush ≥ apply`.** The keepalive path moves only `write`
/// (the received LSN); durability (PR 2.26) is the only thing that advances `flush`/`apply`.
#[derive(Clone, Copy, Debug)]
pub struct StandbyStatus {
    pub write: Lsn,
    pub flush: Lsn,
    pub apply: Lsn,
    pub reply_requested: bool,
}

/// A live `START_REPLICATION` CopyBoth stream over a hand-rolled connection.
#[derive(Debug)]
pub struct ReplicationStream {
    stream: TcpStream,
    rbuf: BytesMut,
    /// The highest LSN we've received (sent as `write` in feedback).
    last_received: Lsn,
    /// The durable baseline (`flush`/`apply`); constant until PR 2.26 advances it.
    durable: Lsn,
    /// Unconditional-feedback cadence (< `wal_sender_timeout`).
    feedback_interval: Duration,
    /// When the next unconditional feedback is due.
    feedback_deadline: Instant,
}

impl ReplicationStream {
    /// Connect, hand-shake, and issue `START_REPLICATION SLOT … LOGICAL <lsn> (proto_version '2',
    /// streaming 'on', publication_names '<publication>')`. `dsn` is parsed for host/port/user/db
    /// (its auth is `trust` in the dev harness).
    ///
    /// # Errors
    ///
    /// Returns [`anyhow::Error`] if the DSN is invalid, TCP/startup negotiation fails, or PostgreSQL
    /// rejects `START_REPLICATION`.
    pub async fn start(
        dsn: &str,
        slot: &str,
        start_lsn: Lsn,
        publication: &str,
    ) -> anyhow::Result<Self> {
        let mut this = Self::connect(dsn).await?;
        this.start_streaming(slot, start_lsn, publication).await?;
        Ok(this)
    }

    /// Open a `replication=database` connection and complete the startup handshake **without** yet
    /// issuing `START_REPLICATION` — the idle state a snapshot export needs (PR 2.29). The caller then
    /// either [`create_replication_slot_export`](Self::create_replication_slot_export) or
    /// [`start_streaming`](Self::start_streaming).
    ///
    /// # Errors
    ///
    /// Returns [`anyhow::Error`] if the DSN is invalid, the TCP connection fails, or the replication
    /// startup handshake is rejected or malformed.
    pub async fn connect(dsn: &str) -> anyhow::Result<Self> {
        let (host, port, user, database) = parse_dsn(dsn)?;
        let stream = TcpStream::connect((host.as_str(), port))
            .await
            .with_context(|| format!("connect to source {host}:{port} for replication"))?;
        let mut this = ReplicationStream {
            stream,
            rbuf: BytesMut::with_capacity(16 * 1024),
            last_received: Lsn::ZERO,
            durable: Lsn::ZERO,
            feedback_interval: DEFAULT_FEEDBACK_INTERVAL,
            feedback_deadline: Instant::now() + DEFAULT_FEEDBACK_INTERVAL,
        };
        this.startup(&user, &database).await?;
        Ok(this)
    }

    /// Issue `START_REPLICATION` from `start_lsn`, seeding the received/durable baselines. On its own
    /// (after [`connect`](Self::connect)) this is the snapshot handoff: stream from `consistent_point`.
    ///
    /// # Errors
    ///
    /// Returns [`anyhow::Error`] if PostgreSQL rejects the replication command or the CopyBoth
    /// response cannot be written, read, or decoded.
    pub async fn start_streaming(
        &mut self,
        slot: &str,
        start_lsn: Lsn,
        publication: &str,
    ) -> anyhow::Result<()> {
        self.last_received = start_lsn;
        self.durable = start_lsn;
        self.feedback_deadline = Instant::now() + self.feedback_interval;
        self.begin_replication(slot, start_lsn, publication).await
    }

    /// `CREATE_REPLICATION_SLOT <slot> LOGICAL pgoutput (SNAPSHOT 'export')` (PR 2.29). Returns
    /// `(consistent_point, snapshot_name)`. **This connection now holds the exported snapshot** — keep
    /// it strictly idle until every backfill session has run `SET TRANSACTION SNAPSHOT`; the next
    /// command on it (e.g. `START_REPLICATION`) ends the snapshot. Unlike
    /// `pg_create_logical_replication_slot()` (the SQL helper), the replication command is the *only*
    /// way to export a `snapshot_name`.
    ///
    /// # Errors
    ///
    /// Returns [`anyhow::Error`] for socket/protocol failures, a PostgreSQL error response, a missing
    /// result column, or an invalid `consistent_point` LSN.
    pub async fn create_replication_slot_export(
        &mut self,
        slot: &str,
    ) -> anyhow::Result<(Lsn, String)> {
        // `NOEXPORT_SNAPSHOT`/`USE_SNAPSHOT` are the alternatives; `EXPORT` is what backfill needs.
        let sql = format!("CREATE_REPLICATION_SLOT {slot} LOGICAL pgoutput (SNAPSHOT 'export')");
        self.send_query(&sql).await?;
        let mut data_row: Option<Vec<Option<String>>> = None;
        loop {
            let (tag, body) = self.read_message().await?;
            match tag {
                // RowDescription 'T' — the column order is fixed and documented; DataRow 'D' carries
                // the values; CommandComplete 'C'; ReadyForQuery 'Z' ends the simple query.
                b'T' | b'C' | b'N' | b'S' => {}
                b'D' => data_row = Some(parse_data_row(&body)?),
                b'Z' => break,
                b'E' => bail!("CREATE_REPLICATION_SLOT failed: {}", error_message(&body)),
                other => bail!(
                    "unexpected reply '{}' to CREATE_REPLICATION_SLOT",
                    other as char
                ),
            }
        }
        let row = data_row.context("CREATE_REPLICATION_SLOT returned no row")?;
        // Columns: 0 = slot_name, 1 = consistent_point, 2 = snapshot_name, 3 = output_plugin.
        let consistent = row
            .get(1)
            .and_then(Clone::clone)
            .context("CREATE_REPLICATION_SLOT row missing consistent_point")?;
        let snapshot_name = row
            .get(2)
            .and_then(Clone::clone)
            .context("CREATE_REPLICATION_SLOT row missing snapshot_name")?;
        // Context, not `anyhow!("…: {e:?}")`: the typed `LsnParseError` already prints the offending
        // text, and keeping it as the cause leaves it in `{:#}` and reachable by `downcast_ref`.
        let consistent_point: Lsn = consistent
            .parse()
            .context("parse the slot's consistent_point as a Postgres LSN")?;
        Ok((consistent_point, snapshot_name))
    }

    /// Override the unconditional-feedback cadence (must stay under the source's `wal_sender_timeout`).
    /// Tests use a long interval to let the server *demand* a reply (`reply_requested`).
    pub fn set_feedback_interval(&mut self, interval: Duration) {
        self.feedback_interval = interval;
        self.feedback_deadline = Instant::now() + interval;
    }

    /// Time remaining until the next unconditional feedback is due. The flush path (PR 2.26) races this
    /// against a slow S3 PUT so keepalive keeps flowing while the read loop is busy — a stalled flush
    /// must never starve the walsender past `wal_sender_timeout` (§1.9). Saturates to zero when overdue.
    pub fn feedback_budget(&self) -> Duration {
        self.feedback_deadline
            .saturating_duration_since(Instant::now())
    }

    /// Read one frame. Sends unconditional feedback whenever the interval elapses (so an idle stream
    /// stays alive), and answers a `reply_requested` keepalive immediately. `None` on stream end.
    ///
    /// ## Cancel safety
    ///
    /// **Not cancel-safe.** Buffered reads retain partial bytes in `self.rbuf`, but this operation
    /// also calls [`Self::send_received_feedback`], whose direct socket write can be left as a
    /// partial standby-status frame if the future is dropped. Callers must pin one `next()` future
    /// and poll it by mutable reference across sibling `select!` branches.
    ///
    /// The remaining drop point is decode-loop cancellation. If it tears a frame, the connection
    /// may remain unrecoverable until disconnect/reconnect: after flushing committed data, the drain
    /// attempts a standby-status frame and `CopyDone`, but those writes are best-effort and cannot
    /// repair framing. Fully eliminating the residual requires resumable outbound staging in a
    /// `wbuf` field mirroring `rbuf`, which is deferred.
    ///
    /// # Errors
    ///
    /// Returns [`anyhow::Error`] if reading or parsing a backend frame fails, PostgreSQL reports an
    /// error, an unexpected CopyBoth message arrives, or periodic feedback cannot be sent.
    pub async fn next(&mut self) -> anyhow::Result<Option<ReplicationMessage>> {
        loop {
            let budget = self
                .feedback_deadline
                .saturating_duration_since(Instant::now());
            let Ok(frame) = tokio::time::timeout(budget, self.read_message()).await else {
                // Feedback due — send it (received LSN as write) and keep waiting.
                self.send_received_feedback(false).await?;
                continue;
            };
            let (tag, body) = frame?;
            match tag {
                b'd' => {
                    if let Some(msg) = self.handle_copy_data(body).await? {
                        return Ok(Some(msg));
                    }
                }
                // CopyDone / ReadyForQuery — the stream ended.
                b'c' | b'Z' => return Ok(None),
                // CommandComplete / NoticeResponse / ParameterStatus — keep going.
                b'C' | b'N' | b'S' => {}
                b'E' => bail!("replication stream error: {}", error_message(&body)),
                other => bail!(
                    "unexpected message '{}' on the CopyBoth stream",
                    other as char
                ),
            }
        }
    }

    /// Send an `'r'` standby status update. Callers (PR 2.26) use this to advance `flush`/`apply` on
    /// durability; the keepalive path uses [`Self::send_received_feedback`].
    ///
    /// ## Cancel safety
    ///
    /// **Not cancel-safe.** Dropping during `write_all` or `flush` can leave a partial frame on the
    /// CopyBoth socket. Callers await it to completion inside a selected branch body, or through a
    /// pinned [`Self::next`] future, so a sibling branch cannot tear the write.
    ///
    /// # Errors
    ///
    /// Returns [`anyhow::Error`] if writing or flushing the status frame to PostgreSQL fails.
    pub async fn send_standby_status(&mut self, s: StandbyStatus) -> anyhow::Result<()> {
        self.stream
            .write_all(&build_standby_status(s))
            .await
            .context("write standby status")?;
        self.stream.flush().await.context("flush standby status")?;
        self.feedback_deadline = Instant::now() + self.feedback_interval;
        Ok(())
    }

    /// Send `CopyDone` and flush — end our side of the CopyBoth stream on a graceful drain (PR 2.28).
    /// The replication **slot is untouched** (never `DROP_REPLICATION_SLOT`); a replacement pod
    /// resumes from `confirmed_flush_lsn`. `CopyDone` is a bare frame: tag `'c'`, Int32 length `4`.
    ///
    /// # Errors
    ///
    /// Returns [`anyhow::Error`] if writing or flushing the `CopyDone` frame fails.
    pub async fn copy_done(&mut self) -> anyhow::Result<()> {
        self.stream
            .write_all(&[b'c', 0, 0, 0, 4])
            .await
            .context("write CopyDone")?;
        self.stream.flush().await.context("flush CopyDone")?;
        Ok(())
    }

    /// The highest received LSN (what the keepalive path reports as `write`).
    pub const fn last_received(&self) -> Lsn {
        self.last_received
    }

    /// Advance the durable (`flush`/`apply`) baseline the periodic keepalive reports — set by the
    /// durability checkpoint (PR 2.26) only after S3 + manifest are durable. Never regresses.
    pub fn set_durable(&mut self, lsn: Lsn) {
        self.durable = self.durable.max(lsn);
    }

    /// The current durable (`confirmed_flush`) baseline.
    pub const fn durable(&self) -> Lsn {
        self.durable
    }

    // ---- internals --------------------------------------------------------------------------

    async fn handle_copy_data(
        &mut self,
        body: Bytes,
    ) -> anyhow::Result<Option<ReplicationMessage>> {
        match body.first().copied() {
            Some(b'w') => {
                // 'w'(1) walStart(8) walEnd(8) clock(8) data(rest)
                if body.len() < 25 {
                    bail!("short XLogData frame ({} bytes)", body.len());
                }
                let wal_start = read_lsn(&body[1..9])?;
                let wal_end = read_lsn(&body[9..17])?;
                let server_clock = read_i64(&body[17..25])?;
                let data = body.slice(25..);
                self.last_received = self.last_received.max(wal_end.max(wal_start));
                Ok(Some(ReplicationMessage::XLogData {
                    wal_start,
                    wal_end,
                    server_clock,
                    data,
                }))
            }
            Some(b'k') => {
                // 'k'(1) walEnd(8) clock(8) replyRequested(1)
                if body.len() < 18 {
                    bail!("short keepalive frame ({} bytes)", body.len());
                }
                let wal_end = read_lsn(&body[1..9])?;
                let server_clock = read_i64(&body[9..17])?;
                let reply_requested = body[17] != 0;
                self.last_received = self.last_received.max(wal_end);
                // A demanded reply is answered *immediately*, not on the next interval.
                if reply_requested {
                    self.send_received_feedback(false).await?;
                }
                Ok(Some(ReplicationMessage::Keepalive {
                    wal_end,
                    server_clock,
                    reply_requested,
                }))
            }
            other => bail!("unknown CopyData sub-type {other:?}"),
        }
    }

    /// Feedback carrying the received LSN as `write`; `flush`/`apply` stay at the durable baseline.
    /// Public so the flush path can pump keepalive while a slow S3 PUT blocks the read loop — the PUT
    /// touches the object store, not this socket, so feedback rides concurrently (§1.9: keepalive is
    /// unconditional, never gated on durability). Resets the feedback deadline on each send.
    ///
    /// ## Cancel safety
    ///
    /// **Not cancel-safe.** It delegates to [`Self::send_standby_status`], whose socket write must
    /// finish once started. The decode loop preserves the enclosing [`Self::next`] future across
    /// heartbeat ticks, and the flush keepalive branch awaits this method to completion.
    ///
    /// # Errors
    ///
    /// Returns [`anyhow::Error`] if [`Self::send_standby_status`] cannot write or flush the feedback.
    pub async fn send_received_feedback(&mut self, reply_requested: bool) -> anyhow::Result<()> {
        self.send_standby_status(StandbyStatus {
            write: self.last_received,
            flush: self.durable,
            apply: self.durable,
            reply_requested,
        })
        .await
    }

    /// Buffered, cancellation-safe read of one backend message (`tag`, `body`). Retained bytes in
    /// `rbuf` survive a cancelled `read_buf`, so the feedback timer can cancel this mid-wait.
    async fn read_message(&mut self) -> anyhow::Result<(u8, Bytes)> {
        loop {
            if let Some(msg) = take_message(&mut self.rbuf) {
                return Ok(msg);
            }
            let n = self
                .stream
                .read_buf(&mut self.rbuf)
                .await
                .context("read from source replication connection")?;
            if n == 0 {
                bail!("source closed the replication connection");
            }
        }
    }

    /// Protocol-3.0 startup with `replication=database`; the dev harness answers `trust` (no SCRAM).
    async fn startup(&mut self, user: &str, database: &str) -> anyhow::Result<()> {
        let startup_message = build_startup(user, database)?;
        self.stream
            .write_all(&startup_message)
            .await
            .context("send StartupMessage")?;
        self.stream.flush().await.context("flush StartupMessage")?;
        loop {
            let (tag, body) = self.read_message().await?;
            match tag {
                b'R' => {
                    let sub = auth_sub_type(&body)?;
                    if sub != 0 {
                        bail!(
                            "source demands auth type {sub}; the dev harness must use trust auth \
                             (SCRAM is not implemented in this spike)"
                        );
                    }
                }
                // ParameterStatus / BackendKeyData / NoticeResponse — ignore.
                b'S' | b'K' | b'N' => {}
                b'Z' => return Ok(()), // ReadyForQuery
                b'E' => bail!("startup failed: {}", error_message(&body)),
                other => bail!("unexpected startup message '{}'", other as char),
            }
        }
    }

    async fn begin_replication(
        &mut self,
        slot: &str,
        start_lsn: Lsn,
        publication: &str,
    ) -> anyhow::Result<()> {
        let sql = format!(
            "START_REPLICATION SLOT {slot} LOGICAL {} \
             (proto_version '2', streaming 'on', publication_names '{publication}')",
            lsn_xy(start_lsn)
        );
        self.send_query(&sql).await?;
        loop {
            let (tag, body) = self.read_message().await?;
            match tag {
                b'W' => return Ok(()), // CopyBothResponse — streaming has begun
                b'N' | b'S' => {}
                b'E' => bail!("START_REPLICATION failed: {}", error_message(&body)),
                other => bail!("unexpected reply '{}' to START_REPLICATION", other as char),
            }
        }
    }

    async fn send_query(&mut self, sql: &str) -> anyhow::Result<()> {
        let capacity = sql
            .len()
            .checked_add(6)
            .context("query too large to buffer for the wire protocol")?;
        let len = sql
            .len()
            .checked_add(5)
            .and_then(|len| u32::try_from(len).ok())
            .with_context(|| {
                format!("query too long for the wire protocol: {} bytes", sql.len())
            })?;
        let mut msg = Vec::with_capacity(capacity);
        msg.push(b'Q');
        msg.extend_from_slice(&len.to_be_bytes());
        msg.extend_from_slice(sql.as_bytes());
        msg.push(0);
        self.stream.write_all(&msg).await.context("send Query")?;
        self.stream.flush().await.context("flush Query")?;
        Ok(())
    }
}

/// Micros since the Postgres epoch (2000-01-01), for the standby-status timestamp.
fn pg_epoch_micros() -> i64 {
    let unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(unix.as_micros())
        .unwrap_or(i64::MAX)
        .saturating_sub(PG_EPOCH_UNIX_SECS * 1_000_000)
}

fn build_startup(user: &str, database: &str) -> anyhow::Result<Vec<u8>> {
    let mut params = Vec::new();
    for (k, v) in [
        ("user", user),
        ("database", database),
        ("replication", "database"),
        ("client_encoding", "UTF8"),
    ] {
        params.extend_from_slice(k.as_bytes());
        params.push(0);
        params.extend_from_slice(v.as_bytes());
        params.push(0);
    }
    params.push(0); // parameter-list terminator
    let len = params
        .len()
        .checked_add(8)
        .context("startup message length overflow")?;
    let wire_len = u32::try_from(len).context("startup message exceeds the Int32 wire limit")?;
    let mut msg = Vec::with_capacity(len);
    msg.extend_from_slice(&wire_len.to_be_bytes());
    msg.extend_from_slice(&196_608u32.to_be_bytes()); // protocol 3.0
    msg.extend_from_slice(&params);
    Ok(msg)
}

/// A whole `'r'` frame: the `'d'` tag, its 4-byte length, and the fixed 34-byte CopyData payload.
/// The protocol fixes every field, so this is a compile-time width, not a capacity hint.
const STANDBY_STATUS_FRAME_BYTES: usize = 39;

/// Build the fixed-width `'r'` frame in a stack array — no field is variable-width, so the feedback
/// path allocates nothing (`copy_done` writes its bare frame the same way). The offsets below are
/// the wire layout; `replication_test::standby_status_frame_layout` reads them back.
fn build_standby_status(s: StandbyStatus) -> [u8; STANDBY_STATUS_FRAME_BYTES] {
    let mut msg = [0u8; STANDBY_STATUS_FRAME_BYTES];
    msg[0] = b'd';
    // CopyData's payload is fixed: one tag + three LSNs + one timestamp + one reply byte = 34 bytes,
    // and the self-inclusive length adds its own 4 bytes but excludes the tag.
    msg[1..5].copy_from_slice(&38_u32.to_be_bytes());
    msg[5] = b'r';
    msg[6..14].copy_from_slice(&s.write.as_u64().to_be_bytes());
    msg[14..22].copy_from_slice(&s.flush.as_u64().to_be_bytes());
    msg[22..30].copy_from_slice(&s.apply.as_u64().to_be_bytes());
    msg[30..38].copy_from_slice(&pg_epoch_micros().to_be_bytes());
    msg[38] = u8::from(s.reply_requested);
    msg
}

/// Parse a `DataRow` ('D') body: `Int16` column count, then per column an `Int32` length (`-1` =
/// NULL) and that many bytes (UTF-8 text values, since walrus never enables binary output).
fn parse_data_row(body: &[u8]) -> anyhow::Result<Vec<Option<String>>> {
    let mut out = Vec::new();
    if body.len() < 2 {
        return Ok(out);
    }
    let ncols = usize::from(u16::from_be_bytes([body[0], body[1]]));
    let mut i = 2;
    for _ in 0..ncols {
        if i + 4 > body.len() {
            break;
        }
        let len = read_i32(&body[i..i + 4])?;
        i += 4;
        if len < 0 {
            out.push(None);
            continue;
        }
        let len = usize::try_from(len).context("negative DataRow length escaped validation")?;
        let Some(end) = i.checked_add(len) else {
            break;
        };
        if end > body.len() {
            break;
        }
        out.push(Some(String::from_utf8_lossy(&body[i..end]).into_owned()));
        i = end;
    }
    Ok(out)
}

/// Take one framed backend message (`tag` + 4-byte self-inclusive length + body) from `buf`, or
/// `None` if a full message is not yet buffered.
fn take_message(buf: &mut BytesMut) -> Option<(u8, Bytes)> {
    if buf.len() < 5 {
        return None;
    }
    let tag = buf[0];
    let len = usize::try_from(u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]])).ok()?;
    let total = len.checked_add(1)?; // tag + (length field + body)
    if buf.len() < total {
        return None;
    }
    let msg = buf.split_to(total).freeze();
    Some((tag, msg.slice(5..)))
}

/// `X/Y` upper-hex LSN — the only form `START_REPLICATION` accepts (not the 16-hex `Display`).
fn lsn_xy(lsn: Lsn) -> String {
    let v = lsn.as_u64();
    format!("{:X}/{:X}", v >> 32, v & 0xFFFF_FFFF)
}

fn fixed<const N: usize>(b: &[u8], what: &str) -> anyhow::Result<[u8; N]> {
    b.try_into()
        .map_err(|_| anyhow!("{what}: expected {N} bytes, got {}", b.len()))
}

fn read_lsn(b: &[u8]) -> anyhow::Result<Lsn> {
    Ok(Lsn::new(u64::from_be_bytes(fixed(b, "read_lsn")?)))
}
fn read_i64(b: &[u8]) -> anyhow::Result<i64> {
    Ok(i64::from_be_bytes(fixed(b, "read_i64")?))
}
fn read_i32(b: &[u8]) -> anyhow::Result<i32> {
    Ok(i32::from_be_bytes(fixed(b, "read_i32")?))
}

/// The `Int32` sub-type of an Authentication ('R') body. `take_message` frames on the 4-byte length
/// header only, so the body it hands back may be shorter than the sub-type field — a truncated frame
/// is a protocol error the handshake reports, never an out-of-bounds slice that aborts the sink.
fn auth_sub_type(body: &[u8]) -> anyhow::Result<i32> {
    let head = body
        .get(..4)
        .with_context(|| format!("short Authentication message ({} bytes)", body.len()))?;
    read_i32(head)
}

/// The `'M'` (human message) field of an ErrorResponse/NoticeResponse body.
fn error_message(body: &[u8]) -> String {
    let mut i = 0;
    while i < body.len() && body[i] != 0 {
        let field_type = body[i];
        let start = i + 1;
        let mut end = start;
        while end < body.len() && body[end] != 0 {
            end += 1;
        }
        if field_type == b'M' {
            return String::from_utf8_lossy(&body[start..end]).into_owned();
        }
        i = end + 1;
    }
    "(no message)".to_string()
}

fn parse_dsn(dsn: &str) -> anyhow::Result<(String, u16, String, String)> {
    let cfg: tokio_postgres::Config = dsn.parse().context("parse source DSN")?;
    let Some(tokio_postgres::config::Host::Tcp(host)) = cfg.get_hosts().first() else {
        bail!("replication DSN needs a TCP host");
    };
    let port = cfg.get_ports().first().copied().unwrap_or(5432);
    let user = cfg
        .get_user()
        .ok_or_else(|| anyhow!("replication DSN needs a user"))?
        .to_string();
    let database = cfg
        .get_dbname()
        .ok_or_else(|| anyhow!("replication DSN needs a dbname"))?
        .to_string();
    Ok((host.clone(), port, user, database))
}

#[cfg(test)]
#[path = "replication_test.rs"]
mod tests;
