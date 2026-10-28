"""Observed Splunk6 event framing; shared magic alone does not identify events."""
import struct
from . import event_lexicon,msidx


class Reader(event_lexicon._Reader):
    def __init__(self,data,*,retain_opaque_trailer=False,limits=None):
        if type(retain_opaque_trailer) is not bool:raise ValueError('opaque trailer policy requires a boolean')
        self._configure_limits(limits)
        self.retain_opaque_trailer=retain_opaque_trailer
        self.data=data;self.size=len(data);self.legacy=True;self.flags=0;self.value_offset=64
        self.header=self.span(0,64)
        if self.header[:8]!=b'Splunk6\0' or self.span(60,4)!=bytes(4):
            raise ValueError('unsupported legacy event header')
        self.nvalues=self._count(self.u32be(8),'legacy event values','values')
        self.nterms=self._count(self.u32be(16),'legacy event terms','terms');self.npostings=self.u32be(12)
        if not self.nterms<=self.npostings<=self.nvalues*self.nterms:raise ValueError('invalid legacy event posting count')
        # Metric Splunk6 arrays use numeric width 64. Require all five observed
        # event array widths/sizes before interpreting any value pairs.
        arrays=[]
        for size_at,width_at,name in ((20,40,'event_time'),(24,44,'source'),(28,48,'sourcetype'),(32,52,'host'),(36,56,'delete')):
            size=self.u32be(size_at);width=self.u32be(width_at)
            logical=(self.nvalues*width+7)//8
            if not 1<=width<=32 or (size not in (logical,(logical+3)//4*4) and not (width==2 and size==((self.nvalues+7)//8)*width) and not (width in (1,2,5,7,8,9,10) and size==(self.nvalues//8+1)*width)):
                raise ValueError('unverified legacy event array width/size; metrics are separate')
            arrays.append((name,size,width))
        self.earliest=self.lowest_address=0xffffffff;self.latest=self.highest_address=0
        for i in range(self.nvalues):
            seconds,address=struct.unpack('>II',self.span(64+i*8,8))
            if seconds>2147483647:raise ValueError('legacy event time outside observed bounds')
            self.earliest=min(self.earliest,seconds);self.latest=max(self.latest,seconds)
            self.lowest_address=min(self.lowest_address,address);self.highest_address=max(self.highest_address,address)
        self.address_width=(self.highest_address-self.lowest_address).bit_length()
        pos=msidx._strict8(64+self.nvalues*8)
        self.sections={'header':{'offset':0,'bytes':64},'values':{'offset':64,'bytes':self.nvalues*8,
            'allocated_bytes':pos-64,'decoded':False,'bounds_decoded':True}}
        self.span(64,pos-64)
        for name,size,width in arrays:
            self.span(pos,size);next_pos=msidx._strict8(pos+size)
            self.sections[name]={'offset':pos,'bytes':size,'logical_bytes':(self.nvalues*width+7)//8,
                'allocated_bytes':next_pos-pos,'bits_per_value':width,'decoded':False}
            logical=(self.nvalues*width+7)//8
            if size not in (logical,(logical+3)//4*4):
                self.sections[name]['allocation_profile']=('observed_value_capacity_rounded_to_8;content_opaque'
                    if width==2 and size==((self.nvalues+7)//8)*width else 'observed_strict_next_8_value_capacity;content_opaque')
            pos=next_pos
        self.plo=pos;self.meta=msidx._strict8(pos+self.nterms*8)
        self.sections['posting_offsets']={'offset':pos,'bytes':self.nterms*8,'allocated_bytes':self.meta-pos}
        self.count_width=self.nvalues.bit_length();self.time_width=32;self.meta_width=96
        self.meta_size=msidx._strict8(self.nterms*12);self.lexicon_offset=self.meta+self.meta_size
        self.span(self.meta,self.meta_size)
        self.sections['term_metadata']={'offset':self.meta,'bytes':self.meta_size,'bits_per_term':96,'encoding':'count-earliest-latest-be32'}
        self._finish_lexicon()
