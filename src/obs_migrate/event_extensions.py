"""Bounded observations about original trailing bytes, never parser acceptance.

Every known value/posting is validated first. An equal decoded membership proves
only that two byte spans represent the same IDs; it does not prove writer intent,
attachment, deletion visibility, or permission to discard either span.
"""
import base64
import hashlib
import struct

from . import event_index, legacy_postings


def membership_sha256(ids):
    """Ordered uint32 big-endian digest, independent of posting serialization."""
    digest = hashlib.sha256()
    for value in ids:
        digest.update(struct.pack('>I', value))
    return digest.hexdigest()


def inspect(path, *, experimental=False, limits=None):
    """Observe a bounded tail without changing strict extraction policy.

    Explicit diagnostic access pins/hashes the complete source, preflights every
    known physical value/posting, and retains the opaque span under existing caps.
    No candidate tail is trusted as an active structure or used to answer queries.
    """
    with event_index.open_index(path, experimental=experimental,
                                retain_opaque_trailer=True, limits=limits) as index:
        reader = index.reader
        report = {'extension_observation_schema': 1,
                  'source_sha256': index.catalog.sha256,
                  'source_bytes': reader.size,
                  'fully_validated_values': reader.nvalues,
                  'fully_validated_posting_memberships': reader.npostings,
                  'classification': 'no_opaque_trailer',
                  'opaque_trailer': None, 'observations': [],
                  'extraction_policy_changed': False,
                  'attachment_validated': False,
                  'native_writer_intent_verified': False,
                  'search_visibility': 'UNVERIFIED',
                  'indexed_search_supported': False,
                  'original_source_required': True}
        tail = getattr(reader, 'opaque_trailer', None)
        if tail is None:
            return report
        report['opaque_trailer'] = dict(tail)
        report['classification'] = 'unresolved_opaque_trailer'
        wire = reader.span(tail['offset'], tail['bytes'])
        # A byte-shape observation only: no claim that a historical event header
        # references the empty native DS header found in the suffix.
        padding = 8 - tail['offset'] % 8
        if wire == bytes(padding) + b'DSMAGIC0' + bytes(8):
            report['classification'] = 'empty_ds_header_shape'
            report['observations'].append({'header_offset': tail['offset'] + padding,
                                           'header_bytes': 16, 'padding_bytes': padding,
                                           'count_words': [0, 0]})
        if not getattr(reader, 'tsidx9', False):
            return report
        # Try existing bounded delta/run decoding once per distinct bitmap count.
        # Counts are already preflighted and their sum is bounded by the index's
        # posting budget. Never infer arbitrary counts or scan for magic offsets.
        attempted = set()
        decoded = None
        for tid, kind in enumerate(reader.posting_kinds):
            if kind != 'bitmap':
                continue
            count = reader.metadata[tid][0]
            if decoded is None and count not in attempted:
                attempted.add(count)
                try:
                    decoded, _ = legacy_postings.decode(wire, count, reader.nvalues,
                                                        label='TSIDX9 trailing observation')
                except ValueError:
                    pass
            if decoded is None or len(decoded) != count:
                continue
            original, _ = index._postings(tid)
            if original != decoded:
                continue
            offset, length = reader.postings[tid]
            report['classification'] = 'equivalent_bitmap_posting'
            report['observations'].append({
                'term_id': tid,
                'term_base64': base64.b64encode(reader.terms[tid]).decode('ascii'),
                'posting_offset': offset, 'posting_bytes': length,
                'posting_sha256': hashlib.sha256(reader.span(offset, length)).hexdigest(),
                'trailer_encoding': 'existing_delta_run_codec',
                'value_count': count,
                'ordered_value_ids_sha256': membership_sha256(decoded),
                'exact_ordered_membership_equal': True})
        index._check()
        return report
