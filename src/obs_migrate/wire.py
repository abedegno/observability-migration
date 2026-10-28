"""Bounded primitives and diagnostic components for observed BOTS journal records."""
import struct


def uvarint(data, offset=0):
    value = 0
    for i in range(10):
        if offset + i >= len(data):
            raise ValueError("truncated unsigned varint")
        byte = data[offset+i]
        if i == 9 and byte > 1:
            raise ValueError("unsigned varint overflow")
        value |= (byte & 127) << (i*7)
        if not byte & 128:
            return value, i+1
    raise ValueError("unterminated unsigned varint")


def signed32(value):
    if not 0 <= value < 1 << 32:
        raise ValueError("index-time difference outside observed uint32 representation")
    return value - (1 << 32) if value & (1 << 31) else value


def event_components(wire, *, allow_extended=False):
    """Retain full wire integers, including the subsecond bit upstream drops."""
    opcode = wire[0]
    length, n = uvarint(wire, 1)
    offset = 1+n
    if offset + length != len(wire):
        raise ValueError("record framing mismatch")
    if opcode <= 2 or (opcode & 4 and not allow_extended):
        raise ValueError("unsupported record header")
    extended_length = None
    if opcode & 4:
        extended_length, n = uvarint(wire, offset)
        offset += n
    if not opcode & 1:
        offset += 20
    if offset+8 > len(wire):
        raise ValueError("truncated stream id")
    stream_id = struct.unpack_from("<Q", wire, offset)[0]
    offset += 8
    result = {"opcode": opcode, "stream_id": stream_id}
    if extended_length is not None:
        result["extended_storage_length"] = extended_length
    for name in ("stream_offset", "stream_sub_offset", "index_delta_wire", "subsecond_wire", "metadata_count"):
        result[name], n = uvarint(wire, offset)
        offset += n
    return result


def subsecond_decimal(wire):
    """Exact decimal fraction for the encoding observed in native BOTS exports.

    The least significant set bit marks leading decimal zeroes; the higher
    bits contain the remaining decimal digits. Preserve the original wire
    integer separately. This function does not convert to nanoseconds or float.
    """
    if not 0 <= wire < 1 << 64:
        raise ValueError("subsecond slot outside uint64")
    if wire == 0:
        return "0"
    leading_zeroes = (wire & -wire).bit_length() - 1
    digits = str(wire >> (leading_zeroes + 1))
    return "0." + "0" * leading_zeroes + digits


def exact_time(seconds, subsecond_wire=0):
    """Unix seconds + nonnegative exact fraction, with no float or rounding.

    The instant is the arithmetic sum (so seconds=-1, fraction=0.25 is -0.75).
    Format interpretation is gated by the caller's observed journal profile.
    """
    if not isinstance(seconds, int) or not -(1 << 63) <= seconds < 1 << 63:
        raise ValueError("timestamp seconds outside int64")
    fraction = subsecond_decimal(subsecond_wire)
    if "." in fraction:
        fraction = fraction.rstrip("0").rstrip(".")
    return {"seconds": seconds, "fraction": fraction}
