namespace py gfs

struct ChunkInfo {
    1: string chunk_handle,
    2: list<string> locations,
    3: string primary
}

struct FileInfo {
    1: string file_name,
    2: i64 file_size,
    3: list<string> chunk_handles
}

service MasterService {
    bool create_file(1: string filename),
    bool delete_file(1: string filename),
    FileInfo get_file_info(1: string filename),
    ChunkInfo get_chunk_handle(1: string filename, 2: i32 chunk_index),
    bool heartbeat(1: string server_id, 2: list<string> chunk_handles)
}

service ChunkService {
    string read(1: string chunk_handle, 2: i32 offset, 3: i32 length),
    bool write(1: string chunk_handle, 2: i32 offset, 3: string data),
    void append(1: string chunk_handle, 2: string data),
    void copy_snapshot(1: string source, 2: string destination)
}
