import uuid
import threading
import time
from metaData import FileMetaData, ChunkMetaData

CHUNK_SIZE = 64 * 1024 * 1024  # 64MB
REPLICATION_FACTOR = 3
LEASE_DURATION = 60  # seconds
LOCK_TIMEOUT = 5  # seconds


class Master:
    def __init__(self):
        self.file_meta_data = {}   # filename -> FileMetaData
        self.chunk_meta_data = {}  # chunk_handle -> ChunkMetaData
        self.chunkservers = {}     # server_id -> last_heartbeat_time
        self.leases = {}           # chunk_handle -> (primary, expiry)
        self.lock = threading.Lock()

    def _acquire_lock(self, timeout=LOCK_TIMEOUT):
        """Acquire lock with timeout to prevent indefinite blocking."""
        acquired = self.lock.acquire(timeout=timeout)
        if not acquired:
            raise TimeoutError("Failed to acquire Master lock within timeout")
        return acquired

    def create_file(self, filename):
        self._acquire_lock()
        try:
            if filename in self.file_meta_data:
                return False
            self.file_meta_data[filename] = FileMetaData(filename, 0, '', [], [])
            return True
        finally:
            self.lock.release()

    def delete_file(self, filename):
        self._acquire_lock()
        try:
            if filename not in self.file_meta_data:
                return False
            file_meta = self.file_meta_data.pop(filename)
            for chunk_handle in file_meta.chunkIDList:
                self.chunk_meta_data.pop(chunk_handle, None)
                self.leases.pop(chunk_handle, None)
            return True
        finally:
            self.lock.release()

    def get_file_info(self, filename):
        self._acquire_lock()
        try:
            return self.file_meta_data.get(filename)
        finally:
            self.lock.release()

    def get_chunk_handle(self, filename, chunk_index):
        self._acquire_lock()
        try:
            if filename not in self.file_meta_data:
                return None
            file_meta = self.file_meta_data[filename]
            # allocate new chunks up to chunk_index
            while len(file_meta.chunkIDList) <= chunk_index:
                chunk_handle = str(uuid.uuid4())
                servers = list(self.chunkservers.keys())[:REPLICATION_FACTOR]
                self.chunk_meta_data[chunk_handle] = ChunkMetaData(
                    chunk_handle, 0, 0, servers
                )
                file_meta.chunkIDList.append(chunk_handle)

            chunk_handle = file_meta.chunkIDList[chunk_index]
            chunk_meta = self.chunk_meta_data[chunk_handle]
            primary = self._get_or_grant_lease(chunk_handle, chunk_meta.location_list)
            return chunk_handle, chunk_meta.location_list, primary
        finally:
            self.lock.release()

    def _get_or_grant_lease(self, chunk_handle, locations):
        """Must be called with lock held."""
        now = time.time()
        if chunk_handle in self.leases:
            primary, expiry = self.leases[chunk_handle]
            if now < expiry and primary in locations:
                self.leases[chunk_handle] = (primary, now + LEASE_DURATION)
                return primary
        if locations:
            primary = locations[0]
            self.leases[chunk_handle] = (primary, now + LEASE_DURATION)
            return primary
        return None

    def heartbeat(self, server_id, chunk_handles):
        self._acquire_lock()
        try:
            self.chunkservers[server_id] = time.time()
            for chunk_handle in chunk_handles:
                if chunk_handle in self.chunk_meta_data:
                    meta = self.chunk_meta_data[chunk_handle]
                    if server_id not in meta.location_list:
                        meta.location_list.append(server_id)
            return True
        finally:
            self.lock.release()
