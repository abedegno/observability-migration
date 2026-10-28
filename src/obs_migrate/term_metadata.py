"""Compact internal count/time triples; no wire format or record schema change."""
from array import array
from collections.abc import Sequence
from operator import index


class TermMetadata(Sequence):
    """Three validated unsigned32 values per term, materialized only on access.

    Array storage is process-local, never serialized. Original native wire spans
    remain owned by the reader. The previous list's indexing/iteration values
    are preserved, including negative indices and list-valued slices.
    """
    __slots__=('_values',)

    def __init__(self):
        self._values=array('I')
        if self._values.itemsize!=4:raise RuntimeError('term metadata requires 32-bit unsigned arrays')

    def append(self,row):
        if len(row)!=3 or any(type(value) is not int or not 0<=value<=0xffffffff for value in row):
            raise ValueError('term metadata requires three unsigned 32-bit integers')
        self._values.extend(row)

    def __len__(self):return len(self._values)//3

    def __getitem__(self,key):
        if isinstance(key,slice):return [self[i] for i in range(*key.indices(len(self)))]
        key=index(key)
        if key<0:key+=len(self)
        if not 0<=key<len(self):raise IndexError('term metadata index out of range')
        start=key*3;values=self._values
        return values[start],values[start+1],values[start+2]

    def __iter__(self):
        values=iter(self._values)
        return zip(values,values,values)

    def __eq__(self,other):
        if isinstance(other,TermMetadata):return self._values==other._values
        if not isinstance(other,Sequence):return NotImplemented
        return len(self)==len(other) and all(a==b for a,b in zip(self,other))

    @property
    def packed_bytes(self):return len(self._values)*self._values.itemsize
