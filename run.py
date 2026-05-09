import sys
import os
import argparse
import threading
import thriftpy2
from thriftpy2.rpc import make_server

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'server'))

_thrift_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'config', 'gfs.thrift'))
gfs_thrift = thriftpy2.load(_thrift_path, module_name='gfs_thrift')

from gfsAPI import MasterHandler, ChunkHandler
from config_parser import GFSConfig


def run_master(config):
    host, port = config.get_master_address()
    handler = MasterHandler()
    server = make_server(gfs_thrift.MasterService, handler, host, port)
    print(f'[Master] listening on {host}:{port}')
    print(f'  chunk_size={config.get("gfs.chunk.size")}  '
          f'replication={config.get("gfs.replication")}  '
          f'lease={config.get("gfs.lease.duration")}s')
    server.serve()


def run_chunkserver(server_id, host, port):
    handler = ChunkHandler(server_id)
    server = make_server(gfs_thrift.ChunkService, handler, host, port)
    print(f'[ChunkServer {server_id}] listening on {host}:{port}')
    server.serve()


def run_cluster(config):
    """Start master + all chunkservers from config (for local testing)."""
    chunkservers = config.get_chunkservers()

    threads = []

    master_thread = threading.Thread(
        target=run_master, args=(config,), daemon=True
    )
    master_thread.start()
    threads.append(master_thread)
    print(f'[Cluster] Master started')

    for cs_host, cs_port, cs_id in chunkservers:
        t = threading.Thread(
            target=run_chunkserver, args=(cs_id, cs_host, cs_port), daemon=True
        )
        t.start()
        threads.append(t)
        print(f'[Cluster] ChunkServer {cs_id} started')

    print(f'[Cluster] All nodes running. Press Ctrl+C to stop.')
    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        print('\n[Cluster] Shutting down.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='mGFS - Mini Google File System')
    parser.add_argument('role', choices=['master', 'chunkserver', 'cluster'],
                        help='master: start master only; '
                             'chunkserver: start one chunkserver; '
                             'cluster: start all nodes from config')
    parser.add_argument('--config', default=None, help='path to config directory')
    parser.add_argument('--host', default=None)
    parser.add_argument('--port', type=int, default=None)
    parser.add_argument('--id', default='cs1', help='chunkserver id')
    args = parser.parse_args()

    config = GFSConfig(args.config)

    if args.role == 'master':
        if args.host:
            config.properties['gfs.master.host'] = args.host
        if args.port:
            config.properties['gfs.master.port'] = str(args.port)
        run_master(config)

    elif args.role == 'chunkserver':
        cs_host = args.host or '127.0.0.1'
        cs_port = args.port or 9091
        run_chunkserver(args.id, cs_host, cs_port)

    elif args.role == 'cluster':
        run_cluster(config)
