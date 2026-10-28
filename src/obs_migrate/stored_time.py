"""Exact nanosecond bounds for verified legacy stored indexed timestamps.

No floats, text ordering, journal-time inference or timestamp rewriting. Current
legacy profiles have at most nine decimal fractional digits. Bounds may span the
signed Int64 seconds domain even though observed native producers cover less.
"""
from dataclasses import dataclass
import re

NANOSECONDS=1_000_000_000
MIN_SECONDS=-(2**63)
MAX_SECONDS=2**63-1


def ticks(seconds,fraction):
    if type(seconds) is not int or not MIN_SECONDS<=seconds<=MAX_SECONDS:
        raise ValueError('stored time seconds require a signed Int64 integer')
    if not isinstance(fraction,str) or not re.fullmatch(r'0(?:\.[0-9]{1,9})?',fraction):
        raise ValueError('stored time fraction requires 0 or 0.<1..9 decimal digits>')
    digits=fraction.partition('.')[2]
    return seconds*NANOSECONDS+int(digits.ljust(9,'0') or '0')


@dataclass(frozen=True)
class TimeBound:
    seconds:int
    fraction:str='0'
    inclusive:bool=True

    def __post_init__(self):
        ticks(self.seconds,self.fraction)
        if type(self.inclusive) is not bool:raise ValueError('time bound inclusive must be bool')

    @property
    def ticks(self):return ticks(self.seconds,self.fraction)

    @classmethod
    def from_ticks(cls,value,*,inclusive=True):
        if type(value) is not int:raise ValueError('time ticks require an integer')
        seconds,fraction=divmod(value,NANOSECONDS)
        return cls(seconds,'0' if not fraction else '0.'+f'{fraction:09d}'.rstrip('0'),inclusive)

    def describe(self):return {'seconds':self.seconds,'fraction':self.fraction,'inclusive':self.inclusive}


@dataclass(frozen=True)
class TimeRange:
    lower:TimeBound|None=None
    upper:TimeBound|None=None

    def __post_init__(self):
        if any(b is not None and not isinstance(b,TimeBound) for b in (self.lower,self.upper)):
            raise ValueError('stored time range requires TimeBound endpoints')

    @property
    def bound_count(self):return sum(b is not None for b in (self.lower,self.upper))

    def contains(self,value):
        return ((self.lower is None or value>self.lower.ticks or (self.lower.inclusive and value==self.lower.ticks)) and
                (self.upper is None or value<self.upper.ticks or (self.upper.inclusive and value==self.upper.ticks)))

    def intersect(self,other):
        def choose(a,b,lower):
            if a is None:return b
            if b is None:return a
            if a.ticks==b.ticks:return TimeBound.from_ticks(a.ticks,inclusive=a.inclusive and b.inclusive)
            return (a if a.ticks>b.ticks else b) if lower else (a if a.ticks<b.ticks else b)
        return TimeRange(choose(self.lower,other.lower,True),choose(self.upper,other.upper,False))

    def describe(self):
        return {'comparison':'integer_nanoseconds;stored_indexed_time',
                'lower':None if self.lower is None else self.lower.describe(),
                'upper':None if self.upper is None else self.upper.describe()}
