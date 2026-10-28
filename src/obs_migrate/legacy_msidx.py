"""Shared allocation bounds observed in native legacy Float64 metric arrays."""
import base64

from .msidx import MsidxError


def numeric_bytes(reader):
    needed=reader.nvalues*8;size=reader.u32be(20)
    # Genuine larger native buffers retain the last 64-byte allocation unit.
    # Smaller originals store exactly the logical length. No arbitrary slack.
    if reader.u32be(40)!=64 or size not in (needed,((needed+63)//64)*64):
        raise MsidxError('unsupported legacy numeric width/array allocation profile')
    return size


def padding_state(reader):
    length=reader.numeric_bytes-reader.nvalues*8
    if not length:return {}
    offset=reader.numeric_offset+reader.nvalues*8
    return {'numeric_array_storage_bytes':reader.numeric_bytes,'numeric_array_padding_offset':offset,
        'numeric_array_padding_wire_base64':base64.b64encode(reader.span(offset,length)).decode()}
