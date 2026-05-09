import threading


class ChunkServer:
    def __init__(self, server_id):
        self.server_id = server_id
        self.chunk_data = {}  # chunk_handle -> bytearray
        self.lock = threading.Lock()

    def read(self, chunk_handle, offset, length):
        with self.lock:
            if chunk_handle not in self.chunk_data:
                return ''
            data = self.chunk_data[chunk_handle]
            return bytes(data[offset:offset + length]).decode('utf-8', errors='replace')

    def write(self, chunk_handle, offset, data):
        with self.lock:
            if chunk_handle not in self.chunk_data:
                self.chunk_data[chunk_handle] = bytearray()
            buf = self.chunk_data[chunk_handle]
            encoded = data.encode('utf-8')
            end = offset + len(encoded)
            if len(buf) < end:
                buf.extend(b'\x00' * (end - len(buf)))
            buf[offset:end] = encoded
            return True

    def append(self, chunk_handle, data):
        with self.lock:
            if chunk_handle not in self.chunk_data:
                self.chunk_data[chunk_handle] = bytearray()
            self.chunk_data[chunk_handle].extend(data.encode('utf-8'))

    def copy_snapshot(self, source, destination):
        with self.lock:
            if source in self.chunk_data:
                self.chunk_data[destination] = bytearray(self.chunk_data[source])
