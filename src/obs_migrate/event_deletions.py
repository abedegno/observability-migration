"""Observed one-bit native delete arrays cross-checked against journal masks.

This validates local physical correspondence only. Native search also depends
on index activity, role/configuration and search-time processing. Original
arrays and masks remain in the pinned source manifest; neither is reconstructed.
"""
import hashlib


def validate(snapshot):
    """Compare every native local-value bit to fully reconciled journal context."""
    if not snapshot._lock.acquire(blocking=False):raise ValueError('event snapshot scan is active')
    try:
        snapshot._check();sources=[];total=deleted=0
        for name,index in snapshot._sources.items():
            index._check();reader=index.reader;section=reader.sections['delete']
            if section['bits_per_value']!=1:raise ValueError('unobserved native event delete-array width')
            count=reader.nvalues;logical=(count+7)//8
            if section['bytes']<logical:raise ValueError('native event delete-array span is too short')
            wire=reader.span(section['offset'],section['bytes']);set_bits=0
            for value_id,address in enumerate(index.addresses):
                ordinal=snapshot._ordinals[name][value_id]
                if snapshot._mask_selectable is not None:
                    expected=not snapshot._mask_selectable[ordinal]
                elif snapshot._journal is not None:
                    expected=address in snapshot._journal._context.deletes
                else:expected=snapshot._table['is_deleted'][ordinal].as_py()
                actual=bool(wire[value_id//8]>>(value_id%8)&1)
                if type(expected) is not bool or actual!=expected:
                    raise ValueError('native delete bitmap disagrees with reconciled journal masks; check original bucket completeness')
                set_bits+=actual
            index._check();total+=count;deleted+=set_bits
            sources.append({'slice_file':name,'source_sha256':index.catalog.sha256,
                'decoder':index.decoder,'flags':reader.flags,'offset':section['offset'],
                'bytes':len(wire),'logical_value_bytes':logical,'sha256':hashlib.sha256(wire).hexdigest(),
                'bits_per_value':1,'bit_order':'local_value_id;least_significant_bit_first_within_byte',
                'values_compared':count,'set_value_bits':set_bits,
                'matches_reconciled_journal_masks':True})
        return {'event_delete_observation_schema':1,'snapshot_sha256':snapshot.snapshot_sha256,
            'sources':sources,'physical_values_compared':total,'deleted_values':deleted,
            'scope':'observed_one_bit_arrays_equal_complete_journal_mask_annotations;not_native_search_visibility',
            'indexed_search_supported':False,'search_visibility':'UNVERIFIED'}
    finally:snapshot._lock.release()


def validate_smartstore_delta(snapshot, expected_masks):
    """Observed immutable TSIDX8/flags0 zero base plus receipt-bound CSV deltas.

    Unlike local native agreement, this explicitly does not equate the original
    base bits to final deletion state. Never rewrite those bytes. Complete mask
    address/time/metadata reconciliation and physical linkage precede this call.
    """
    if not snapshot._lock.acquire(blocking=False):raise ValueError('event snapshot scan is active')
    try:
        snapshot._check()
        actual={name:{'bytes':len(wire),'sha256':hashlib.sha256(wire).hexdigest()}
                for name,wire in snapshot._files.items() if name.startswith('rawdata/deletes/')}
        if not actual or actual!=expected_masks:
            raise ValueError('SmartStore delta masks differ from receipt-bound object evidence')
        if snapshot._mask_selectable is None:raise ValueError('SmartStore delta requires complete mask selection state')
        sources=[];total=0
        for name,index in snapshot._sources.items():
            index._check();reader=index.reader;section=reader.sections['delete']
            if reader.span(0,8)!=b'_TSIDX8\0' or reader.flags!=0 or section['bits_per_value']!=1:
                raise ValueError('unobserved immutable SmartStore base index profile')
            count=reader.nvalues;logical=(count+7)//8
            if section['bytes']<logical:raise ValueError('SmartStore base delete span is too short')
            wire=reader.span(section['offset'],section['bytes'])
            if any(wire):raise ValueError('immutable SmartStore base requires original all-zero delete bytes')
            total+=count
            sources.append({'slice_file':name,'source_sha256':index.catalog.sha256,
                'decoder':index.decoder,'flags':reader.flags,'offset':section['offset'],
                'bytes':len(wire),'sha256':hashlib.sha256(wire).hexdigest(),
                'bits_per_value':1,'values_compared':count,'set_value_bits':0})
        if total!=len(snapshot._mask_selectable):raise ValueError('SmartStore base/mask physical count mismatch')
        deleted=total-sum(snapshot._mask_selectable)
        return {'event_delete_observation_schema':2,'snapshot_sha256':snapshot.snapshot_sha256,
            'policy':'observed_immutable_smartstore_base_plus_receipt_bound_journal_masks',
            'sources':sources,'physical_values_compared':total,'native_base_deleted_values':0,
            'journal_mask_deleted_values':deleted,'native_base_equals_final_mask':deleted==0,
            'receipt_bound_masks':actual,
            'scope':'all_zero_original_tsidx8_flags0_base_and_fully_reconciled_csv_delta;not_native_search_visibility',
            'indexed_search_supported':False,'search_visibility':'UNVERIFIED'}
    finally:snapshot._lock.release()
