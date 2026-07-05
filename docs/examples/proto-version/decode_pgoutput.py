#!/usr/bin/env python3
"""
decode_pgoutput.py — a throwaway, stdlib-only pgoutput decoder.

NOT production code. Its only job is to make the binary pgoutput stream legible so the
captures in docs/proto-version.md are accurate, and to give the golden-vector unit tests
(test_decode_pgoutput.py) a thing to assert against. It parses the pgoutput
logical-replication message format (proto_version up to 4) into structured dicts, and renders
those dicts as one human-readable line each.

Two input modes:

  # (a) hex lines — one complete message per line (the SQL-function path):
  psql -At -c "SELECT encode(data,'hex') FROM pg_logical_slot_peek_binary_changes(
       'slot_pg',NULL,NULL,'proto_version','2','publication_names','pub','streaming','on')" \
    | python3 decode_pgoutput.py

  # (b) raw concatenated stream from a live walsender (pg_recvlogical -f -):
  pg_recvlogical ... --plugin=pgoutput --start -o proto_version=2 \
       -o publication_names=pub -o streaming=on -f - \
    | python3 decode_pgoutput.py --stream

Structured API (used by the tests):
  parse_hex(hexstr, streaming=False) -> dict         # one message
  parse_message(reader, ctx) -> dict                 # ctx = [in_stream_bool]
  render(msg) -> str                                 # dict -> the display line

Byte layouts follow:
  https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html
"""
import sys
from datetime import datetime, timedelta, timezone

PG_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)


class Reader:
    """Cursor over a bytes buffer with the primitive readers pgoutput uses."""
    def __init__(self, buf):
        self.b = buf
        self.i = 0

    def remaining(self):
        return len(self.b) - self.i

    def byte1(self):
        c = self.b[self.i:self.i + 1]
        self.i += 1
        return c.decode("latin1")

    def int8(self):
        v = self.b[self.i]
        self.i += 1
        return v

    def int16(self):
        v = int.from_bytes(self.b[self.i:self.i + 2], "big")
        self.i += 2
        return v

    def int32(self):
        v = int.from_bytes(self.b[self.i:self.i + 4], "big")
        self.i += 4
        return v

    def int64(self):
        v = int.from_bytes(self.b[self.i:self.i + 8], "big")
        self.i += 8
        return v

    def string(self):
        end = self.b.index(b"\x00", self.i)
        s = self.b[self.i:end].decode("utf-8", "replace")
        self.i = end + 1
        return s


def lsn(v):
    return f"{v >> 32:X}/{v & 0xFFFFFFFF:X}"


def ts(v):
    return (PG_EPOCH + timedelta(microseconds=v)).isoformat()


# ---------------------------------------------------------------------------
# PARSE — bytes -> structured dict
# ---------------------------------------------------------------------------

def parse_tuple(r):
    """TupleData: Int16 ncols, then per column a format byte + optional length+value.
    Returns a list of {"fmt": "n"|"u"|"t"|"b", value/hex}."""
    ncols = r.int16()
    cols = []
    for _ in range(ncols):
        fmt = r.byte1()
        if fmt == "n":
            cols.append({"fmt": "n"})                          # SQL NULL
        elif fmt == "u":
            cols.append({"fmt": "u"})                          # unchanged TOAST — value NOT sent
        elif fmt == "t":
            ln = r.int32(); val = r.b[r.i:r.i + ln]; r.i += ln
            cols.append({"fmt": "t", "value": val.decode("utf-8", "replace")})
        elif fmt == "b":
            ln = r.int32(); val = r.b[r.i:r.i + ln]; r.i += ln
            cols.append({"fmt": "b", "hex": val.hex()})
        else:
            raise ValueError(f"bad TupleData format byte {fmt!r} (misaligned parse?)")
    return cols


# message types that carry the per-message xid prefix, but ONLY inside a streamed txn
# (Relation, Type, Insert, Update, Delete, Truncate, Message). We track streamed context.
XID_PREFIXED = set("RYIUDTM")


def parse_message(r, ctx):
    """Parse one pgoutput message. ctx is a 1-element list holding the 'inside a streamed
    block' flag, which Stream Start/Stop toggle and which decides whether the per-message xid
    prefix is present."""
    t = r.byte1()
    xid = r.int32() if (t in XID_PREFIXED and ctx[0]) else None

    if t == "B":
        return {"type": "begin", "final_lsn": r.int64(), "commit_ts": r.int64(), "xid": r.int32()}
    if t == "C":
        return {"type": "commit", "flags": r.int8(),
                "commit_lsn": r.int64(), "end_lsn": r.int64(), "commit_ts": r.int64()}
    if t == "O":
        return {"type": "origin", "commit_lsn": r.int64(), "name": r.string()}
    if t == "R":
        oid = r.int32(); ns = r.string(); rel = r.string()
        ident = r.byte1(); ncols = r.int16(); cols = []
        for _ in range(ncols):
            fl = r.int8(); cn = r.string(); ct = r.int32(); tm = r.int32()
            cols.append({"key": bool(fl & 1), "name": cn, "type_oid": ct,
                         "typmod": -1 if tm == 0xFFFFFFFF else tm})
        return {"type": "relation", "xid": xid, "oid": oid, "namespace": ns, "name": rel,
                "replident": ident, "columns": cols}
    if t == "Y":
        return {"type": "type", "xid": xid, "oid": r.int32(),
                "namespace": r.string(), "name": r.string()}
    if t == "I":
        oid = r.int32(); r.byte1()  # 'N'
        return {"type": "insert", "xid": xid, "relation_oid": oid, "new": parse_tuple(r)}
    if t == "U":
        oid = r.int32(); marker = r.byte1()
        old_kind = None; old = None
        if marker in ("K", "O"):
            old_kind = marker; old = parse_tuple(r); r.byte1()  # consume the 'N'
        new = parse_tuple(r)
        return {"type": "update", "xid": xid, "relation_oid": oid,
                "old_kind": old_kind, "old": old, "new": new}
    if t == "D":
        oid = r.int32(); marker = r.byte1()
        return {"type": "delete", "xid": xid, "relation_oid": oid,
                "old_kind": marker, "old": parse_tuple(r)}
    if t == "T":
        nrel = r.int32(); opt = r.int8()
        return {"type": "truncate", "xid": xid,
                "cascade": bool(opt & 1), "restart_identity": bool(opt & 2),
                "relations": [r.int32() for _ in range(nrel)]}
    if t == "M":
        flags = r.int8(); msg_lsn = r.int64(); prefix = r.string()
        ln = r.int32(); content = r.b[r.i:r.i + ln]; r.i += ln
        return {"type": "message", "xid": xid, "transactional": bool(flags & 1),
                "lsn": msg_lsn, "prefix": prefix, "content": content}
    # ---- streaming frames (v2+) ----
    if t == "S":
        ctx[0] = True
        return {"type": "stream_start", "xid": r.int32(), "first_segment": r.int8()}
    if t == "E":
        ctx[0] = False
        return {"type": "stream_stop"}
    if t == "c":
        return {"type": "stream_commit", "xid": r.int32(), "flags": r.int8(),
                "commit_lsn": r.int64(), "end_lsn": r.int64(), "commit_ts": r.int64()}
    if t == "A":
        # Under streaming='on' (v2/v3) a Stream Abort is exactly: 'A' + top_xid + sub_xid.
        # The abort_lsn/abort_ts Int64 pair is added ONLY under streaming='parallel' (v4);
        # this harness uses streaming='on', so we do not read them (can't be inferred from
        # bytes alone). Add them here if you switch the capture to streaming='parallel'.
        top = r.int32(); sub = r.int32()
        return {"type": "stream_abort", "top_xid": top, "sub_xid": sub, "whole_txn": top == sub}
    # ---- two-phase (v3+) ----
    if t == "b":
        return {"type": "begin_prepare", "prepare_lsn": r.int64(), "end_lsn": r.int64(),
                "prepare_ts": r.int64(), "xid": r.int32(), "gid": r.string()}
    if t == "P":
        return {"type": "prepare", "flags": r.int8(), "prepare_lsn": r.int64(),
                "end_lsn": r.int64(), "prepare_ts": r.int64(), "xid": r.int32(), "gid": r.string()}
    if t == "K":
        return {"type": "commit_prepared", "flags": r.int8(), "commit_lsn": r.int64(),
                "end_lsn": r.int64(), "commit_ts": r.int64(), "xid": r.int32(), "gid": r.string()}
    if t == "r":
        return {"type": "rollback_prepared", "flags": r.int8(), "end_lsn": r.int64(),
                "rollback_end_lsn": r.int64(), "prepare_ts": r.int64(), "rollback_ts": r.int64(),
                "xid": r.int32(), "gid": r.string()}
    if t == "p":
        return {"type": "stream_prepare", "flags": r.int8(), "prepare_lsn": r.int64(),
                "end_lsn": r.int64(), "prepare_ts": r.int64(), "xid": r.int32(), "gid": r.string()}
    return {"type": "unknown", "byte": t, "remaining": r.remaining()}


def parse_hex(hexstr, streaming=False):
    """Parse a single message from a hex string (test convenience)."""
    return parse_message(Reader(bytes.fromhex(hexstr)), [streaming])


def parse_stream(data, ctx=None):
    """Parse a raw pg_recvlogical byte stream into a list of messages. pg_recvlogical -f
    appends a 0x0a separator after each message (it is built for line-based test_decoding and
    adds it even for binary pgoutput); pgoutput messages are self-delimiting, so we skip
    exactly one separator between them."""
    ctx = ctx if ctx is not None else [False]
    r = Reader(data)
    out = []
    while r.remaining() > 0:
        if r.b[r.i] == 0x0A:
            r.i += 1
            continue
        out.append(parse_message(r, ctx))
    return out


# ---------------------------------------------------------------------------
# RENDER — structured dict -> one display line (kept byte-identical to prior output)
# ---------------------------------------------------------------------------

def render_tuple(cols):
    out = []
    for c in cols:
        if c["fmt"] == "n":
            out.append("NULL")
        elif c["fmt"] == "u":
            out.append("<unchanged-TOAST>")
        elif c["fmt"] == "t":
            shown = c["value"]
            if len(shown) > 40:
                shown = shown[:37] + "..."
            out.append(f"'{shown}'")
        else:  # 'b'
            out.append("0x" + c["hex"])
    return "[" + ", ".join(out) + "]"


def render(m):
    t = m["type"]
    pre = f"xid={m['xid']} " if m.get("xid") is not None else ""
    if t == "begin":
        return f"BEGIN         final_lsn={lsn(m['final_lsn'])} commit_ts={ts(m['commit_ts'])} xid={m['xid']}"
    if t == "commit":
        return f"COMMIT        flags={m['flags']} commit_lsn={lsn(m['commit_lsn'])} end_lsn={lsn(m['end_lsn'])} commit_ts={ts(m['commit_ts'])}"
    if t == "origin":
        return f"ORIGIN        commit_lsn={lsn(m['commit_lsn'])} name={m['name']!r}"
    if t == "relation":
        cols = ", ".join(
            f"{'KEY ' if c['key'] else ''}{c['name']}(oid={c['type_oid']},mod={c['typmod']})"
            for c in m["columns"])
        return f"RELATION      {pre}oid={m['oid']} {m['namespace']}.{m['name']} replident={m['replident']!r} cols=[{cols}]"
    if t == "type":
        return f"TYPE          {pre}oid={m['oid']} {m['namespace']}.{m['name']}"
    if t == "insert":
        return f"INSERT        {pre}rel={m['relation_oid']} new={render_tuple(m['new'])}"
    if t == "update":
        oldstr = f" old[{m['old_kind']}]={render_tuple(m['old'])}" if m["old_kind"] else ""
        return f"UPDATE        {pre}rel={m['relation_oid']}{oldstr} new={render_tuple(m['new'])}"
    if t == "delete":
        return f"DELETE        {pre}rel={m['relation_oid']} old[{m['old_kind']}]={render_tuple(m['old'])}"
    if t == "truncate":
        flags = []
        if m["cascade"]: flags.append("CASCADE")
        if m["restart_identity"]: flags.append("RESTART IDENTITY")
        return f"TRUNCATE      {pre}opts={'|'.join(flags) or 'none'} rels={m['relations']}"
    if t == "message":
        kind = "transactional" if m["transactional"] else "non-transactional"
        return f"MESSAGE       {pre}{kind} lsn={lsn(m['lsn'])} prefix={m['prefix']!r} content={m['content']!r}"
    if t == "stream_start":
        return f"STREAM START  xid={m['xid']} first_segment={m['first_segment']}"
    if t == "stream_stop":
        return "STREAM STOP"
    if t == "stream_commit":
        return f"STREAM COMMIT xid={m['xid']} flags={m['flags']} commit_lsn={lsn(m['commit_lsn'])} end_lsn={lsn(m['end_lsn'])} commit_ts={ts(m['commit_ts'])}"
    if t == "stream_abort":
        kind = "WHOLE-TXN abort" if m["whole_txn"] else "SUBTRANSACTION rollback"
        return f"STREAM ABORT  top_xid={m['top_xid']} sub_xid={m['sub_xid']}  <- {kind}"
    if t == "begin_prepare":
        return f"BEGIN PREPARE prepare_lsn={lsn(m['prepare_lsn'])} end_lsn={lsn(m['end_lsn'])} ts={ts(m['prepare_ts'])} xid={m['xid']} gid={m['gid']!r}"
    if t == "prepare":
        return f"PREPARE       prepare_lsn={lsn(m['prepare_lsn'])} end_lsn={lsn(m['end_lsn'])} ts={ts(m['prepare_ts'])} xid={m['xid']} gid={m['gid']!r}"
    if t == "commit_prepared":
        return f"COMMIT PREP   commit_lsn={lsn(m['commit_lsn'])} end_lsn={lsn(m['end_lsn'])} ts={ts(m['commit_ts'])} xid={m['xid']} gid={m['gid']!r}"
    if t == "rollback_prepared":
        return f"ROLLBACK PREP end_lsn={lsn(m['end_lsn'])} rollback_lsn={lsn(m['rollback_end_lsn'])} prepare_ts={ts(m['prepare_ts'])} rollback_ts={ts(m['rollback_ts'])} xid={m['xid']} gid={m['gid']!r}"
    if t == "stream_prepare":
        return f"STREAM PREPARE prepare_lsn={lsn(m['prepare_lsn'])} end_lsn={lsn(m['end_lsn'])} ts={ts(m['prepare_ts'])} xid={m['xid']} gid={m['gid']!r}"
    return f"?? unknown message type {m['byte']!r} (remaining {m['remaining']} bytes)"


def main():
    stream_mode = "--stream" in sys.argv[1:]
    ctx = [False]  # mutable "are we inside a streamed block" flag
    if stream_mode:
        data = sys.stdin.buffer.read()
        r = Reader(data)
        n = 0
        while r.remaining() > 0:
            # pg_recvlogical -f appends a 0x0a separator after every message (it is built
            # for line-based test_decoding and adds it even for binary pgoutput). pgoutput
            # messages are self-delimiting, so skip exactly one separator between them.
            if r.b[r.i] == 0x0A:
                r.i += 1
                continue
            start = r.i
            try:
                print(render(parse_message(r, ctx)))
            except Exception as e:  # noqa: BLE001 — scratch tool, surface & stop
                print(f"!! parse error at byte {start}: {e}")
                break
            n += 1
        print(f"# {n} messages", file=sys.stderr)
    else:
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            try:
                print(render(parse_message(Reader(bytes.fromhex(line)), ctx)))
            except Exception as e:  # noqa: BLE001 — scratch tool
                print(f"!! parse error on {line[:24]}...: {e}")


if __name__ == "__main__":
    main()
