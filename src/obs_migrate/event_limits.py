"""Explicit bounded event-reader budgets; None preserves original defaults.

These are logical/count budgets, not a process RSS limit. They do not relax
format validation or native-search semantics. Upper bounds are deliberately
finite; use replace() to select stricter experimental workload budgets.
"""
from dataclasses import dataclass,asdict,replace
import re

MiB=1024**2
CAPS={'values':1_000_000,'terms':2_000_000,'postings':64_000_000,
      'lexicon_bytes':64*MiB,'events':1_000_000,'bucket_memberships':64_000_000,
      'bucket_terms':3_000_000,'decompressed_bytes':512*MiB}


@dataclass(frozen=True)
class EventLimits:
    name:str='explicit-default-events-v1'
    values:int=100_000
    terms:int=100_000
    postings:int=5_000_000
    lexicon_bytes:int=16*MiB
    events:int=100_000
    bucket_memberships:int=5_000_000
    bucket_terms:int=1_000_000
    decompressed_bytes:int=256*MiB

    def __post_init__(self):
        if not isinstance(self.name,str) or not re.fullmatch(r'[a-z][a-z0-9-]{0,62}',self.name):raise ValueError('event resource profile requires a bounded ASCII name')
        for field,cap in CAPS.items():
            value=getattr(self,field)
            if type(value) is not int or not 1<=value<=cap:raise ValueError('event resource '+field+' outside bounded profile limits')

    def describe(self):return {'event_resource_schema':1,**asdict(self),'memory_scope':'logical/count limits;not process RSS;source and Arrow caps unchanged'}


LARGE_EVENTS=replace(EventLimits(),name='bounded-large-events-v1',**CAPS)


def validate(limits):
    if limits is not None and type(limits) is not EventLimits:raise ValueError('event limits require an EventLimits profile')
    return limits
