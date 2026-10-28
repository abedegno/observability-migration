"""Lazy Arrow Dataset bridge, backed solely by immutable captured native bytes.

Selected native rows are encoded into a bounded, immutable Parquet byte buffer
on the first provider access. Later scans reuse that buffer. This is lazy row
emission and a Parquet bridge, not direct native SQL predicate translation.
"""
import io
import threading

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as fs
import pyarrow.parquet as pq


class _LimitedBuffer(io.BytesIO):
    def __init__(self,limit):super().__init__();self.limit=limit
    def write(self,data):
        if self.tell()+len(data)>self.limit:raise ValueError('capture Parquet byte limit exceeded')
        return super().write(data)


class _Handler(fs.FileSystemHandler):
    def __init__(self,selection,limit,reserve):
        self.selection=selection;self.limit=limit;self.wire=None;self.arrow_bytes=0;self.materializations=0
        self._lock=threading.Lock();self.reserve=reserve

    def _ensure(self):
        with self._lock:
            if self.wire is None:
                self.selection.capture.check();size=0
                with _LimitedBuffer(self.limit) as stream:
                    with pq.ParquetWriter(stream,self.selection.reader.SCHEMA,compression='zstd',write_statistics=True) as writer:
                        rows=self.selection.records()
                        try:
                            for batch in self.selection.reader.batches(rows,max_rows=256):
                                size+=batch.nbytes
                                if size>self.limit:raise ValueError('capture dataset Arrow byte limit exceeded')
                                writer.write_batch(batch)
                        finally:rows.close()
                    wire=stream.getvalue()
                self.reserve(max(size,len(wire)))
                self.wire=wire;self.arrow_bytes=size;self.materializations+=1
            return self.wire

    def get_type_name(self):return 'obs-immutable-capture'
    def normalize_path(self,path):return path
    def get_file_info(self,paths):
        return [fs.FileInfo(path,fs.FileType.File,size=len(self.wire) if self.wire is not None else None)
                if path=='points.parquet' else fs.FileInfo(path,fs.FileType.NotFound) for path in paths]
    def get_file_info_selector(self,selector):raise OSError('capture dataset has one explicit file')
    def open_input_file(self,path):
        if path!='points.parquet':raise FileNotFoundError(path)
        return pa.BufferReader(self._ensure())
    def open_input_stream(self,path):return self.open_input_file(path)
    def _readonly(self,*args,**kwargs):raise OSError('immutable capture dataset is read-only')
    create_dir=delete_dir=delete_dir_contents=delete_root_dir_contents=delete_file=move=copy_file=open_output_stream=open_append_stream=_readonly


def dataset(selection,limit,reserve=lambda size:None):
    handler=_Handler(selection,limit,reserve)
    filesystem=fs.PyFileSystem(handler)
    value=ds.FileSystemDataset.from_paths(['points.parquet'],schema=selection.reader.SCHEMA,
        format=ds.ParquetFileFormat(),filesystem=filesystem)
    return value,handler
