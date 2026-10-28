"""Derived exact stored event/index time candidates over immutable physical rows."""
from array import array
from bisect import bisect_left,bisect_right
from contextlib import closing
from .stored_time import TimeRange
import re

FIELDS=('event_time','index_time')
MAX_TIME_INDEX_BYTES=32*1024**2


class EventTimeIndex:
    def __init__(self,snapshot):
        if snapshot.backend!='journal':raise ValueError('derived event time requires journal backend')
        times={name:[] for name in FIELDS};work={};precision={name:0 for name in FIELDS}
        with closing(snapshot.projected_records(('record_ordinal',*FIELDS),stats=work)) as rows:
            for row in rows:
                for name in FIELDS:
                    value=row[name]
                    if value is None:continue
                    seconds=value['seconds'];fraction=value['fraction']
                    if type(seconds) is not int or not isinstance(fraction,str) or not re.fullmatch(r'0(?:\.[0-9]{1,63})?',fraction):
                        raise ValueError('derived event time outside exact stored fraction profile')
                    digits=fraction.partition('.')[2];precision[name]=max(precision[name],len(digits))
                    number=seconds*10**9+int(digits[:9].ljust(9,'0') or '0')
                    tail=any(d!='0' for d in digits[9:])
                    if not -(2**63)<=number<2**63:
                        raise ValueError('derived event time outside packed Int64 nanosecond profile')
                    times[name].append((number,row['record_ordinal'],tail))
        self._fields={};size=0
        for name,values in times.items():
            values.sort()
            numbers=array('q',(v for v,_,_ in values));ordinals=array('I',(i for _,i,_ in values));tails=bytes(t for _,_,t in values)
            size+=numbers.itemsize*len(numbers)+ordinals.itemsize*len(ordinals)+len(tails)
            if size>MAX_TIME_INDEX_BYTES:raise ValueError('derived event time byte limit exceeded')
            self._fields[name]=(numbers,ordinals,tails)
        snapshot._check();self.snapshot_sha256=snapshot.snapshot_sha256
        self._report={'derived_time_schema':1,'snapshot_sha256':self.snapshot_sha256,
            'fields':list(FIELDS),'semantics':'physical_exact_stored_time_against_nanosecond_bounds;subnanosecond_remainders_retained;NULL_excluded;duplicates_retained',
            'validation':'canonical_snapshot_projected_rows;no_float_or_timestamp_replacement',
            'maximum_fraction_digits':precision,'retained_index_bytes':size,'construction_work':work,
            'encoding':'sorted_floor_Int64_nanoseconds,uint32_physical_ordinal,and_nonzero_subnanosecond_remainder_byte_per_field',
            'memory_accounting':'packed arrays only;not temporary sorting,Python object overhead or process RSS',
            'native_search_visibility':'UNVERIFIED'}

    def report(self):
        return {**self._report,'fields':list(FIELDS),'construction_work':dict(self._report['construction_work']),'maximum_fraction_digits':dict(self._report['maximum_fraction_digits'])}

    def candidates(self,field,bound):
        if field not in FIELDS or not isinstance(bound,TimeRange):raise ValueError('derived event time requires a known field and TimeRange')
        numbers,ordinals,tails=self._fields[field];lower=bound.lower;upper=bound.upper
        start=0 if lower is None else bisect_left(numbers,lower.ticks)
        stop=len(numbers) if upper is None else (bisect_right if upper.inclusive else bisect_left)(numbers,upper.ticks)
        # Bounds lie on integer nanoseconds, but an original fraction may lie
        # strictly between them. Test the endpoint floor plus retained remainder;
        # do not round or discard original fractional digits.
        result=[]
        for i in range(start,stop):
            if lower is not None and numbers[i]==lower.ticks and not lower.inclusive and not tails[i]:continue
            if upper is not None and numbers[i]==upper.ticks and upper.inclusive and tails[i]:continue
            result.append(ordinals[i])
        return sorted(result)
