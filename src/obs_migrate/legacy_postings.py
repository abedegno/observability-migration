"""Observed bounded literal/delta/run postings, shared by verified profiles.

Original toolkit codec factored from the working MSIDX1 reader. Callers supply
validated cardinality/source spans; no format is inferred from arbitrary bytes.
"""
def uint32(wire,pos=0,*,label='posting',error_type=ValueError):
    value=0
    for shift in range(0,35,7):
        if pos>=len(wire):raise error_type(label+' truncated posting integer')
        byte=wire[pos];pos+=1
        if shift==28 and byte>15:raise error_type(label+' posting integer overflow')
        value|=(byte&127)<<shift
        if byte<128:
            if shift and byte==0:raise error_type(label+' noncanonical posting integer')
            return value,pos
    raise error_type(label+' invalid posting integer')


def decode(wire,expected,nvalues,*,label="MSIDX1",error_type=ValueError):
    pos=0;ids=[];previous=0;has_runs=False
    while pos<len(wire):
        if ids and wire[pos]==0:
            has_runs=True;pos+=1;count,pos=uint32(wire,pos,label=label,error_type=error_type)
            if not 1<=count<=expected-len(ids):raise error_type(label+' invalid posting run')
            if previous+count>=nvalues:raise error_type(label+' posting run outside values')
            ids.extend(range(previous+1,previous+count+1));previous+=count
        else:
            delta,pos=uint32(wire,pos,label=label,error_type=error_type);point=previous+delta
            if point>=nvalues or (ids and point<=previous):
                raise error_type(label+' posting outside values or non-increasing')
            ids.append(point);previous=point
        if len(ids)>expected:raise error_type(label+' posting cardinality mismatch')
    if len(ids)!=expected:raise error_type(label+' posting cardinality mismatch')
    return ids,has_runs


def bitmap(wire,expected,nvalues):
    """Observed TSIDX9 absolute base plus bytewise low-bit-first membership."""
    base,pos=uint32(wire,label='TSIDX9 bitmap')
    if base>=nvalues or not 1<=len(wire)-pos<=(nvalues-base+7)//8:
        raise ValueError('TSIDX9 bitmap base/span outside values')
    ids=[]
    for byte_id,byte in enumerate(wire[pos:]):
        while byte:
            low=byte&-byte;point=base+byte_id*8+low.bit_length()-1
            if point>=nvalues:raise ValueError('TSIDX9 bitmap set bit outside values')
            ids.append(point)
            if len(ids)>expected:raise ValueError('TSIDX9 bitmap cardinality mismatch')
            byte^=low
    if len(ids)!=expected:raise ValueError('TSIDX9 bitmap cardinality mismatch')
    return ids,False
