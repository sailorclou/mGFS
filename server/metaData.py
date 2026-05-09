class PacketMetaData:
    def __init__(self, packet_id, chunk_handle, data):
        self.packet_id = packet_id
        self.chunk_handle = chunk_handle
        self.data = data

class ChunkMetaData:
    def __init__(self, chunk_handle, chunk_size, version_number, location_list):
        self.chunk_handle = chunk_handle
        self.chunk_size = chunk_size
        self.version_number = version_number
        self.location_list = location_list


class FileMetaData:
    def __init__(self, file_name, file_size, owner, ACL, chunkIDList):
        self.file_name = file_name
        self.file_size = file_size
        self.owner = owner
        self.ACL = ACL
        self.chunkIDList = chunkIDList