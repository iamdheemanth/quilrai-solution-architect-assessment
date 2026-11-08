import json

import pytest

from llm_gateway.sse import SSEError, SSEEvent, SSEParser

BOM = b"\xef\xbb\xbf"
U2028 = "\u2028".encode()
SAMPLE = (
    BOM
    + b": keepalive\r\n"
    + b"event: ping\r\ndata: one\r\n\r\n"
    + b"data: line1\rdata: line2\n\r"
    + b'data: {"c":"a'
    + U2028
    + b'b","m":"'
    + "caf\u00e9 \u4e2d".encode()
    + b'"}\n\n'
    + b"id: 7\nretry: 5\ndata:x\n\ndata: y\n\ndata: z\r\n\r\n"
    + b": tail\n"
)


def parse_all(chunks):
    p = SSEParser()
    out = []
    for c in chunks:
        out += p.feed(c)
    out += p.close()
    return out


def test_basic_events():
    evs = parse_all([SAMPLE])
    kinds = [(e.kind, e.event, e.data) for e in evs]
    assert kinds[0] == ("comment", "", "")
    assert kinds[1] == ("data", "ping", "one")
    assert kinds[2] == ("data", "", "line1\nline2")
    assert json.loads(evs[3].data)["c"] == "a\u2028b"
    assert evs[3].data.endswith('"}')
    assert [e.data for e in evs[4:]] == ["x", "y", "z", ""]
    assert evs[-1].kind == "comment"


def test_split_invariance_every_split_and_bytewise():
    whole = parse_all([SAMPLE])
    assert len(whole) == 8
    for i in range(len(SAMPLE) + 1):
        assert parse_all([SAMPLE[:i], SAMPLE[i:]]) == whole, i
    assert parse_all([SAMPLE[i : i + 1] for i in range(len(SAMPLE))]) == whole


def test_u2028_single_event():
    raw = b'data: {"c":"a' + U2028 + b'b"}\n\n'
    evs = parse_all([raw])
    assert len(evs) == 1
    assert json.loads(evs[0].data)["c"] == "a\u2028b"


def test_raw_fidelity_ends_with_blank_line():
    data = SAMPLE + b"data: end\n\n"
    for chunks in ([data], [data[i : i + 1] for i in range(len(data))]):
        evs = parse_all(chunks)
        assert b"".join(e.raw for e in evs) == data[len(BOM) :]


def test_crlf_split_raw_keeps_lf_with_event():
    evs = parse_all([b"data: a\r\n\r", b"\ndata: b\n\n"])
    assert [e.raw for e in evs] == [b"data: a\r\n\r\n", b"data: b\n\n"]


def test_trailing_cr_event_held_until_next_feed():
    p = SSEParser()
    assert p.feed(b"data: a\n\r") == []
    evs = p.feed(b"x")
    assert [e.raw for e in evs] == [b"data: a\n\r"]


def test_bom_spanning_feeds_and_single_strip():
    evs = parse_all([BOM[:1], BOM[1:2], BOM[2:] + b"data: a\n\n"])
    assert [e.data for e in evs] == ["a"]
    assert evs[0].raw == b"data: a\n\n"
    evs = parse_all([BOM + BOM + b"data: a\n\n"])
    assert evs == []  # second BOM corrupts the field name; no data


def test_comment_event():
    evs = parse_all([b": hi\n"])
    assert evs == [SSEEvent("comment", "", "", b": hi\n")]


def test_dataless_event_not_dispatched():
    assert parse_all([b"event: x\n\n\n\nid: 1\n\n"]) == []


def test_data_space_stripping():
    evs = parse_all([b"data:  a\ndata\ndata:b\n\n"])
    assert evs[0].data == " a\n\nb"


def test_incomplete_trailing_dropped_on_close():
    p = SSEParser()
    assert p.feed(b"data: a\n") == []
    assert p.close() == []


def test_invalid_utf8_replaced():
    evs = parse_all([b"data: \xff\n\n"])
    assert evs[0].data == "\ufffd"


def test_line_limit():
    p = SSEParser(max_line=10)
    with pytest.raises(SSEError):
        p.feed(b"data: " + b"a" * 20)
    p = SSEParser(max_line=10)
    with pytest.raises(SSEError):
        p.feed(b"data: " + b"a" * 20 + b"\n")


def test_event_limit():
    p = SSEParser(max_line=100, max_event=50)
    with pytest.raises(SSEError):
        for _ in range(10):
            p.feed(b"data: " + b"a" * 20 + b"\n")
