"""Derived exact byte routing postings from fully validated immutable state.

These are toolkit indexes over physical journal rows, not native TSIDX search
visibility or tokenizer semantics. SQL residuals remain required.
"""
from array import array
from types import MappingProxyType
from .journal_access import STATE

FIELDS={'host':(3,3),'source':(4,4),'sourcetype':(5,5)}
MAX_ROUTING_BYTES=32*1024**2


class RoutingIndex:
    def __init__(self,snapshot):
        if not snapshot._lock.acquire(blocking=False):raise ValueError('event snapshot scan is active')
        try:self._build(snapshot)
        finally:snapshot._lock.release()

    def _build(self,snapshot):
        snapshot._check();journal=snapshot._journal
        if journal is None:raise ValueError('derived event routing requires journal backend')
        journal.check();fields={};key_bytes=posting_bytes=0
        for name,(dictionary_key,state_field) in FIELDS.items():
            values=journal._fields.get(dictionary_key,())
            encoded={i+1:value.encode('utf8','surrogateescape') for i,value in enumerate(values)}
            postings={}
            for ordinal in range(len(journal._states)//STATE.size):
                active=STATE.unpack_from(journal._states,ordinal*STATE.size)[state_field]
                if not active:continue
                if active not in encoded:raise ValueError('derived routing dictionary reference outside source')
                postings.setdefault(encoded[active],array('I')).append(ordinal)
            frozen={}
            for value,ids in postings.items():
                wire=ids.tobytes();key_bytes+=len(value);posting_bytes+=len(wire)
                if key_bytes+posting_bytes>MAX_ROUTING_BYTES:raise ValueError('derived event routing byte limit exceeded')
                frozen[value]=wire
            fields[name]=MappingProxyType(frozen)
        self._fields=MappingProxyType(fields);self.snapshot_sha256=snapshot.snapshot_sha256
        self._report={'derived_routing_schema':1,'snapshot_sha256':self.snapshot_sha256,
            'semantics':'physical_exact_binary_routing_equality;NULL_not_equal_to_literal;no_normalization',
            'validation':'canonical_initial_journal_state_and_append_only_dictionaries',
            'retained_posting_bytes':posting_bytes,'retained_key_bytes':key_bytes,
            'field_values':{name:len(values) for name,values in fields.items()},
            'memory_accounting':'packed uint32 postings and logical key bytes;not Python object overhead or process RSS',
            'native_search_visibility':'UNVERIFIED'}

    def report(self):return {**self._report,'field_values':dict(self._report['field_values'])}

    def candidates(self,field,value):
        if field not in FIELDS or type(value) is not bytes:raise ValueError('derived routing requires a known field and exact bytes')
        result=array('I');result.frombytes(self._fields[field].get(value,b''))
        return result
