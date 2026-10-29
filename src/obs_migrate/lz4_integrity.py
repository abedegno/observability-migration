"""Bounded standard LZ4 frame validation using the existing python-lz4 codec.

The library owns frame headers, block decoding, checksums and end markers. This
adapter requires complete frames and rejects trailing garbage instead of relying
on a file reader's permissive concatenation/unused-data behavior. Legacy LZ4 and
skippable frames are not native profiles qualified by these fixtures.
"""
MAGIC=b'\x04\x22\x4d\x18'
CHUNK_BYTES=64*1024

class FrameIntegrityError(ValueError):pass


def preflight(file,*,max_output_bytes=None):
    from lz4.frame import LZ4FrameDecompressor
    if max_output_bytes is not None and (type(max_output_bytes) is not int or max_output_bytes<1):
        raise FrameIntegrityError('decompressed byte limit must be a positive integer')
    frames=total=maximum=0
    pending=b''
    try:
        while True:
            if not pending:pending=file.read(CHUNK_BYTES)
            if not pending:break
            if len(pending)<4:pending+=file.read(4-len(pending))
            if pending[:4]!=MAGIC:raise FrameIntegrityError('unrecognized or truncated bytes after LZ4 frame')
            with LZ4FrameDecompressor() as decoder:
                chunk=pending;pending=b''
                while True:
                    output=decoder.decompress(chunk,max_length=CHUNK_BYTES)
                    total+=len(output);maximum=max(maximum,len(output))
                    if max_output_bytes is not None and total>max_output_bytes:
                        raise FrameIntegrityError('decompressed LZ4 bytes exceed resource limit')
                    if decoder.eof:
                        pending=decoder.unused_data or b''
                        frames+=1;break
                    if decoder.needs_input:
                        chunk=file.read(CHUNK_BYTES)
                        if not chunk:raise FrameIntegrityError('truncated LZ4 frame')
                    else:chunk=b''
        if not frames:raise FrameIntegrityError('LZ4 source contains no complete data frame')
    except RuntimeError as error:
        raise FrameIntegrityError('LZ4 library integrity failure: '+str(error)) from error
    return {'frames':frames,'decompressed_bytes':total,'maximum_returned_chunk_bytes':maximum,
            'integrity':'complete_standard_frames;library_EOF_and_checksums;bounded_output_chunks'}
