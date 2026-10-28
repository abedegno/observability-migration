"""Published Zstd transport tests; no proprietary journal is fabricated."""
import io
import random
import struct
from pathlib import Path
import pytest
import zstandard as z
from obs_migrate import journal
from obs_migrate.zstd_integrity import preflight,FrameIntegrityError,MAX_WINDOW_BYTES


def skip(data):return struct.pack('<II',0x184d2a50,len(data))+data


@pytest.mark.parametrize('checksum',[False,True])
@pytest.mark.parametrize('content',[False,True])
@pytest.mark.parametrize('data',[b'',b'hello',b'x'*200000,random.Random(123).randbytes(200000)])
def test_standard_raw_compressed_checksum_and_unknown_content_frames(data,checksum,content):
    wire=z.ZstdCompressor(write_checksum=checksum,write_content_size=content).compress(data)
    result=preflight(io.BytesIO(wire))
    assert result['decompressed_bytes']==len(data) and result['frames']==1
    assert result['checksum_frames']==int(checksum) and result['maximum_returned_chunk_bytes']<=128*1024


def test_standard_rle_block_uses_one_payload_byte():
    # RFC8878 single-segment frame,ten-byte output,one RLE block. This is an
    # ordinary published compression container, not a Splunk binary fixture.
    wire=b'\x28\xb5\x2f\xfd\x20\x0a'+((10<<3)|3).to_bytes(3,'little')+b'A'
    assert z.ZstdDecompressor().decompress(wire)==b'A'*10
    assert preflight(io.BytesIO(wire))['decompressed_bytes']==10


def test_concatenated_and_skippable_frames_preserve_full_transport(tmp_path):
    a=z.ZstdCompressor(write_checksum=True).compress(b'first')
    b=z.ZstdCompressor(write_checksum=True,write_content_size=False).compress(b'second')
    wire=skip(b'prefix')+a+skip(b'middle')+b+skip(b'tail')
    result=preflight(io.BytesIO(wire));assert result['frames']==2 and result['skippable_frames']==3
    path=tmp_path/'opaque-extension';path.write_bytes(wire)
    with journal.open_source(path) as (stream,_,compression,_):assert compression=='zstd' and stream.read()==b'firstsecond'
    assert path.read_bytes()==wire


@pytest.mark.parametrize('mode',['checksum','no-checksum','unknown-size','rle','multi'])
def test_every_incomplete_standard_frame_prefix_is_rejected(mode):
    c=z.ZstdCompressor(write_checksum=mode!='no-checksum',write_content_size=mode!='unknown-size')
    wire=c.compress(b'transport payload'*100)
    if mode=='rle':wire=b'\x28\xb5\x2f\xfd\x20\x0a'+((10<<3)|3).to_bytes(3,'little')+b'A'
    if mode=='multi':wire+=z.ZstdCompressor(write_checksum=True).compress(b'other')
    # A complete first frame is a valid standalone prefix of concatenated data;
    # every other truncation must fail, even when stream_reader returns data.
    complete_first=len(c.compress(b'transport payload'*100)) if mode=='multi' else -1
    for length in range(len(wire)):
        if length==complete_first:continue
        with pytest.raises(FrameIntegrityError):preflight(io.BytesIO(wire[:length]))


@pytest.mark.parametrize('suffix',[b'garbage',b'\x28',skip(b'valid')[:-1],b'\x50\x2a\x4d\x18\xff\xff\xff\xff'])
def test_extra_or_truncated_tail_refuses_before_any_journal_read(tmp_path,suffix):
    wire=z.ZstdCompressor(write_checksum=True).compress(b'transport data')+suffix
    path=tmp_path/'bad';path.write_bytes(wire)
    with pytest.raises(journal.JournalError,match='frame-integrity'):
        with journal.open_source(path):pytest.fail('preflight must fail before yielding a stream')


def test_checksum_reserved_block_window_and_output_budgets_are_strict(tmp_path):
    wire=z.ZstdCompressor(write_checksum=True).compress(b'A'*1048576)
    bad=bytearray(wire);bad[-1]^=1
    with pytest.raises(FrameIntegrityError,match='integrity'):preflight(io.BytesIO(bad))
    bad=bytearray(wire);header=z.frame_header_size(wire);bad[header]|=6
    with pytest.raises(FrameIntegrityError,match='reserved'):preflight(io.BytesIO(bad))
    with pytest.raises(FrameIntegrityError,match='decompressed byte limit'):preflight(io.BytesIO(wire),max_output_bytes=100)
    for invalid in (-1,True,'1'):
        with pytest.raises(FrameIntegrityError,match='limit'):preflight(io.BytesIO(wire),max_output_bytes=invalid)
    params=z.ZstdCompressionParameters.from_level(1,window_log=26,write_content_size=False)
    # No large output allocation: a streaming header can declare the window.
    obj=z.ZstdCompressor(compression_params=params).compressobj();header_wire=obj.compress(b'A')+obj.flush()
    assert z.get_frame_parameters(header_wire).window_size>MAX_WINDOW_BYTES
    with pytest.raises(FrameIntegrityError,match='window'):preflight(io.BytesIO(header_wire))


def test_empty_or_skippable_only_is_not_a_data_source():
    for wire in (b'',skip(b'only metadata')):
        with pytest.raises(FrameIntegrityError,match='no data frame'):preflight(io.BytesIO(wire))


def test_short_reads_do_not_weaken_frame_eof_checks():
    class Short(io.BytesIO):
        def read(self,size=-1):return super().read(min(size,2) if size>=0 else 2)
    data=b'ordinary transport'*1000;wire=z.ZstdCompressor(write_checksum=True).compress(data)
    assert preflight(Short(wire))['decompressed_bytes']==len(data)
    with pytest.raises(FrameIntegrityError):preflight(Short(wire[:-1]))
