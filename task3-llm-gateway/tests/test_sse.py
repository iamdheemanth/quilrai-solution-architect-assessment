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


def fields(evs):
    return [(e.kind, e.event, e.data) for e in evs]


LF_SAMPLE = (
    BOM
    + b": keepalive\n"
    + b"event: ping\ndata: one\n\n"
    + b"data: line1\ndata: line2\n\n"
    + b'data: {"c":"a'
    + U2028
    + b'b","m":"'
    + "café 中".encode()
    + b'"}\n\n'
    + b"id: 7\nretry: 5\ndata:x\n\ndata: y\n\n: tail\n"
)


def test_split_invariance_every_split_and_bytewise():
    whole = parse_all([SAMPLE])
    assert len(whole) == 8
    expected = fields(whole)
    for i in range(len(SAMPLE) + 1):
        assert fields(parse_all([SAMPLE[:i], SAMPLE[i:]])) == expected, i
    bytewise = [SAMPLE[i : i + 1] for i in range(len(SAMPLE))]
    assert fields(parse_all(bytewise)) == expected


def test_split_invariance_exact_raw_for_lf_only_input():
    whole = parse_all([LF_SAMPLE])
    assert len(whole) == 7
    for i in range(len(LF_SAMPLE) + 1):
        assert parse_all([LF_SAMPLE[:i], LF_SAMPLE[i:]]) == whole, i
    bytewise = [LF_SAMPLE[i : i + 1] for i in range(len(LF_SAMPLE))]
    assert parse_all(bytewise) == whole


def test_u2028_single_event():
    raw = b'data: {"c":"a' + U2028 + b'b"}\n\n'
    evs = parse_all([raw])
    assert len(evs) == 1
    assert json.loads(evs[0].data)["c"] == "a\u2028b"


def test_raw_fidelity_ends_with_blank_line():
    # Precondition: no dataless events (their bytes are not preserved), and a
    # terminator split as CR|LF across feeds loses the LF, so bytewise feeding
    # uses the LF-only fixture; the mixed fixture is fed whole.
    data = SAMPLE + b"data: end\n\n"
    evs = parse_all([data])
    assert b"".join(e.raw for e in evs) == data[len(BOM) :]
    lf = LF_SAMPLE + b"data: end\n\n"
    evs = parse_all([lf[i : i + 1] for i in range(len(lf))])
    assert b"".join(e.raw for e in evs) == lf[len(BOM) :]


def emit_indexes(chunks):
    p = SSEParser()
    return [(i, ev) for i, c in enumerate(chunks) for ev in p.feed(c)]


def test_cr_only_bytewise_emits_on_final_cr_feed():
    stream = b"data: a\r\rdata: b\r\r: ping\rdata: c\r\r"
    got = emit_indexes([stream[i : i + 1] for i in range(len(stream))])
    assert [(i, e.kind, e.data) for i, e in got] == [
        (8, "data", "a"),
        (17, "data", "b"),
        (24, "comment", ""),
        (33, "data", "c"),
    ]
    assert len(stream) == 34 and stream[8:9] == b"\r" and stream[24:25] == b"\r"


def test_crlf_split_after_cr_emits_on_first_feed_and_drops_lf():
    p = SSEParser()
    first = p.feed(b"data: a\r\n\r")
    assert [e.data for e in first] == ["a"]
    second = p.feed(b"\ndata: b\r\n\r\n")
    assert [e.raw for e in second] == [b"data: b\r\n\r\n"]
    assert first[0].raw == b"data: a\r\n\r"


def test_comment_ending_in_cr_emitted_immediately():
    p = SSEParser()
    evs = p.feed(b": hi\r")
    assert [e.raw for e in evs] == [b": hi\r"]
    assert p.feed(b"\n") == []


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
