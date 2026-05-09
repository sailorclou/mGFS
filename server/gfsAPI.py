import sys
import os
import thriftpy2

sys.path.insert(0, os.path.dirname(__file__))

_thrift_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'gfs.thrift'))
gfs_thrift = thriftpy2.load(_thrift_path, module_name='gfs_thrift')

from master import Master
from chunkserver import ChunkServer


class MasterHandler:
    def __init__(self):
        self._master = Master()

    def create_file(self, filename):
        return self._master.create_file(filename)

    def delete_file(self, filename):
        return self._master.delete_file(filename)

    def get_file_info(self, filename):
        info = self._master.get_file_info(filename)
        if info is None:
            return gfs_thrift.FileInfo(file_name='', file_size=0, chunk_handles=[])
        return gfs_thrift.FileInfo(
            file_name=info.file_name,
            file_size=info.file_size,
            chunk_handles=list(info.chunkIDList)
        )

    def get_chunk_handle(self, filename, chunk_index):
        result = self._master.get_chunk_handle(filename, chunk_index)
        if result is None:
            return gfs_thrift.ChunkInfo(chunk_handle='', locations=[], primary='')
        chunk_handle, locations, primary = result
        return gfs_thrift.ChunkInfo(
            chunk_handle=chunk_handle,
            locations=list(locations),
            primary=primary or ''
        )

    def heartbeat(self, server_id, chunk_handles):
        return self._master.heartbeat(server_id, chunk_handles)


class ChunkHandler:
    def __init__(self, server_id):
        self._cs = ChunkServer(server_id)

    def read(self, chunk_handle, offset, length):
        return self._cs.read(chunk_handle, offset, length)

    def write(self, chunk_handle, offset, data):
        return self._cs.write(chunk_handle, offset, data)

    def append(self, chunk_handle, data):
        self._cs.append(chunk_handle, data)

    def copy_snapshot(self, source, destination):
        self._cs.copy_snapshot(source, destination)
