import sys
import os
import uuid
import thriftpy2
from thriftpy2.rpc import make_client

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'server'))

_thrift_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'gfs.thrift'))
gfs_thrift = thriftpy2.load(_thrift_path, module_name='gfs_thrift')

from metaData import PacketMetaData
from stream import DataQueue, ACKQueue

CHUNK_SIZE = 64 * 1024 * 1024  # 64MB


class GFSClient:
    def __init__(self, master_host='127.0.0.1', master_port=9090):
        self.master_host = master_host
        self.master_port = master_port
        self.data_queue = DataQueue()
        self.ack_queue = ACKQueue()
        self.chunk_server_ports = {}  # server_id -> (host, port)

    def _get_master(self):
        return make_client(gfs_thrift.MasterService, self.master_host, self.master_port)

    def _get_chunkserver(self, server_id):
        if server_id not in self.chunk_server_ports:
            return None
        host, port = self.chunk_server_ports[server_id]
        return make_client(gfs_thrift.ChunkService, host, port)

    def register_chunkserver(self, server_id, host, port):
        self.chunk_server_ports[server_id] = (host, port)

    def create(self, filename):
        master = self._get_master()
        return master.create_file(filename)

    def delete(self, filename):
        master = self._get_master()
        return master.delete_file(filename)

    def read(self, filename, offset=0, length=None):
        master = self._get_master()
        file_info = master.get_file_info(filename)
        if not file_info.file_name:
            return None

        result = []
        chunk_index = offset // CHUNK_SIZE
        chunk_offset = offset % CHUNK_SIZE

        while True:
            chunk_info = master.get_chunk_handle(filename, chunk_index)
            if not chunk_info.chunk_handle:
                break

            read_size = CHUNK_SIZE - chunk_offset
            if length is not None:
                bytes_read = sum(len(s) for s in result)
                read_size = min(read_size, length - bytes_read)
                if read_size <= 0:
                    break

            data = self._read_from_replica(chunk_info, chunk_offset, read_size)
            if data is None or data == '':
                break
            result.append(data)
            chunk_index += 1
            chunk_offset = 0

        return ''.join(result)

    def _read_from_replica(self, chunk_info, offset, length):
        for server_id in chunk_info.locations:
            cs = self._get_chunkserver(server_id)
            if cs is None:
                continue
            try:
                return cs.read(chunk_info.chunk_handle, offset, length)
            except Exception:
                continue
        return None

    def write(self, filename, data, offset=0):
        master = self._get_master()
        chunk_index = offset // CHUNK_SIZE
        chunk_offset = offset % CHUNK_SIZE
        pos = 0

        while pos < len(data):
            chunk_info = master.get_chunk_handle(filename, chunk_index)
            if not chunk_info.chunk_handle:
                return False

            write_size = min(CHUNK_SIZE - chunk_offset, len(data) - pos)
            chunk_data = data[pos:pos + write_size]

            success = self._write_pipeline(chunk_info, chunk_offset, chunk_data)
            if not success:
                return False

            pos += write_size
            chunk_index += 1
            chunk_offset = 0

        return True

    def _write_pipeline(self, chunk_info, offset, data):
        packet_id = str(uuid.uuid4())
        packet = PacketMetaData(packet_id, chunk_info.chunk_handle, data)
        self.data_queue.enqueue(packet)
        self.ack_queue.expect(packet_id, chunk_info.locations)

        # Phase 1: push data to all replicas (pipeline)
        for server_id in chunk_info.locations:
            cs = self._get_chunkserver(server_id)
            if cs is None:
                continue
            try:
                cs.write(chunk_info.chunk_handle, offset, data)
                self.ack_queue.ack(packet_id, server_id)
            except Exception:
                pass

        # Phase 2: check acks
        if self.ack_queue.is_complete(packet_id):
            self.ack_queue.remove(packet_id)
            self.data_queue.dequeue()
            return True

        # Pipeline failure - attempt recovery
        failed = self.ack_queue.get_failed_servers(packet_id)
        success = self.stream_recovery(packet, chunk_info, failed)
        self.ack_queue.remove(packet_id)
        self.data_queue.dequeue()
        return success

    def append(self, filename, data):
        master = self._get_master()
        file_info = master.get_file_info(filename)
        if not file_info.file_name:
            return False

        chunk_index = max(0, len(file_info.chunk_handles) - 1)
        chunk_info = master.get_chunk_handle(filename, chunk_index)
        if not chunk_info.chunk_handle:
            return False

        packet_id = str(uuid.uuid4())
        self.ack_queue.expect(packet_id, chunk_info.locations)

        # Send append to primary first, then secondaries
        for server_id in chunk_info.locations:
            cs = self._get_chunkserver(server_id)
            if cs is None:
                continue
            try:
                cs.append(chunk_info.chunk_handle, data)
                self.ack_queue.ack(packet_id, server_id)
            except Exception:
                pass

        success = self.ack_queue.is_complete(packet_id)
        self.ack_queue.remove(packet_id)
        return success

    def stream_recovery(self, packet, chunk_info, failed_servers):
        """Re-establish pipeline excluding failed servers."""
        healthy = [s for s in chunk_info.locations if s not in failed_servers]
        if not healthy:
            return False

        for server_id in healthy:
            cs = self._get_chunkserver(server_id)
            if cs is None:
                continue
            try:
                cs.write(packet.chunk_handle, 0, packet.data)
            except Exception:
                return False
        return True
