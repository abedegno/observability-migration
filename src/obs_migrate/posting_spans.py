"""Compact process-local posting coordinates, not a proprietary wire format."""
from array import array
from collections.abc import Sequence
from operator import index

MISSING=0xffffffff


class PostingSpans(Sequence):
    """Unsigned32 offset/length pairs with the existing (None,None) sentinel.

    Public event readers cap original files at 256 MiB, so valid coordinates
    cannot collide with the reserved sentinel. Original native operands and
    file bytes remain in the reader; this derived array is never serialized.
    """
    __slots__=('_values',)

    def __init__(self):
        self._values=array('I')
        if self._values.itemsize!=4:raise RuntimeError('posting spans require 32-bit unsigned arrays')

    def append(self,row):
        if len(row)!=2:raise ValueError('posting span requires an offset/length pair')
        if row[0] is None and row[1] is None:
            self._values.extend((MISSING,MISSING));return
        if any(type(value) is not int or not 0<=value<MISSING for value in row):
            raise ValueError('posting span requires two unsigned32 coordinates or two nulls')
        self._values.extend(row)

    def __len__(self):return len(self._values)//2

    def __getitem__(self,key):
        if isinstance(key,slice):return [self[i] for i in range(*key.indices(len(self)))]
        key=index(key)
        if key<0:key+=len(self)
        if not 0<=key<len(self):raise IndexError('posting span index out of range')
        at=key*2;left,right=self._values[at],self._values[at+1]
        return (None,None) if left==MISSING else (left,right)

    def __iter__(self):
        values=iter(self._values)
        return ((None,None) if left==MISSING else (left,right) for left,right in zip(values,values))

    def __eq__(self,other):
        if isinstance(other,PostingSpans):return self._values==other._values
        if not isinstance(other,Sequence):return NotImplemented
        return len(self)==len(other) and all(a==b for a,b in zip(self,other))

    @property
    def packed_bytes(self):return len(self._values)*self._values.itemsize
