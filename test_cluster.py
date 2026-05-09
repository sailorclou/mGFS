"""
mGFS Cluster Integration Test
Implements test cases from TEST_DOC.md
"""
import sys
import os
import time
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'server'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'client'))

import thriftpy2
from thriftpy2.rpc import make_server, make_client

_thrift_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'config', 'gfs.thrift'))
gfs_thrift = thriftpy2.load(_thrift_path, module_name='gfs_thrift')

from gfsAPI import MasterHandler, ChunkHandler

MASTER_HOST = '127.0.0.1'
MASTER_PORT = 19090
CHUNKSERVERS = [
    ('127.0.0.1', 19091, 'cs1'),
    ('127.0.0.1', 19092, 'cs2'),
    ('127.0.0.1', 19093, 'cs3'),
]

passed = 0
failed = 0


def report(test_id, test_name, condition, detail=''):
    global passed, failed
    if condition:
        passed += 1
        print(f'  [PASS] {test_id} {test_name}')
    else:
        failed += 1
        print(f'  [FAIL] {test_id} {test_name} -- {detail}')


# ============ Cluster Setup ============

def start_master():
    handler = MasterHandler()
    server = make_server(gfs_thrift.MasterService, handler, MASTER_HOST, MASTER_PORT)
    server.serve()


def start_chunkserver(server_id, host, port):
    handler = ChunkHandler(server_id)
    server = make_server(gfs_thrift.ChunkService, handler, host, port)
    server.serve()


def get_master():
    return make_client(gfs_thrift.MasterService, MASTER_HOST, MASTER_PORT)


def get_cs(host, port):
    return make_client(gfs_thrift.ChunkService, host, port)


def setup_cluster():
    print('Starting cluster...')
    t = threading.Thread(target=start_master, daemon=True)
    t.start()

    for host, port, sid in CHUNKSERVERS:
        t = threading.Thread(target=start_chunkserver, args=(sid, host, port), daemon=True)
        t.start()

    time.sleep(1)
    print('Cluster ready.\n')


# ============ Test Cases ============

def test_cluster_startup():
    print('=== 3.1 Cluster Startup ===')
    try:
        master = get_master()
        master.create_file('__ping__')
        master.delete_file('__ping__')
        report('T01', 'Master startup', True)
    except Exception as e:
        report('T01', 'Master startup', False, str(e))

    for host, port, sid in CHUNKSERVERS:
        try:
            cs = get_cs(host, port)
            cs.read('__ping__', 0, 1)
            report('T02', f'ChunkServer {sid} startup', True)
        except Exception as e:
            report('T02', f'ChunkServer {sid} startup', False, str(e))

    try:
        master = get_master()
        result = master.heartbeat('cs1', [])
        report('T03', 'Heartbeat registration', result)
    except Exception as e:
        report('T03', 'Heartbeat registration', False, str(e))


def test_file_operations():
    print('\n=== 3.2 File Operations ===')
    master = get_master()

    r = master.create_file('/test.txt')
    report('T04', 'Create file', r is True)

    r = master.create_file('/test.txt')
    report('T05', 'Duplicate create rejected', r is False)

    info = master.get_file_info('/test.txt')
    report('T06', 'Get file info', info.file_name == '/test.txt')

    info = master.get_file_info('/no_such_file')
    report('T07', 'Get nonexistent file', info.file_name == '')

    r = master.delete_file('/test.txt')
    report('T08', 'Delete file', r is True)

    r = master.delete_file('/no_such_file')
    report('T09', 'Delete nonexistent file', r is False)


def test_chunk_allocation():
    print('\n=== 3.3 Chunk Allocation ===')
    master = get_master()

    for sid in ['cs1', 'cs2', 'cs3']:
        master.heartbeat(sid, [])

    master.create_file('/data.bin')

    chunk = master.get_chunk_handle('/data.bin', 0)
    report('T10', 'Allocate chunk', chunk.chunk_handle != '')

    report('T11', 'Replica locations', len(chunk.locations) > 0,
           f'locations={chunk.locations}')

    report('T12', 'Primary election',
           chunk.primary != '' and chunk.primary in chunk.locations,
           f'primary={chunk.primary}')

    chunk2 = master.get_chunk_handle('/data.bin', 1)
    report('T13', 'Multiple chunks',
           chunk2.chunk_handle != '' and chunk2.chunk_handle != chunk.chunk_handle)

    master.delete_file('/data.bin')


def test_data_read_write():
    print('\n=== 3.4 Data Read/Write ===')
    cs = get_cs(*CHUNKSERVERS[0][:2])

    r = cs.write('chunk_rw_test', 0, 'hello')
    report('T14', 'Write data', r is True)

    data = cs.read('chunk_rw_test', 0, 5)
    report('T15', 'Read data', data == 'hello', f'got "{data}"')

    cs.write('chunk_rw_test', 5, ' world')
    report('T16', 'Offset write', True)

    data = cs.read('chunk_rw_test', 0, 11)
    report('T17', 'Full read', data == 'hello world', f'got "{data}"')

    cs.append('chunk_rw_test', '!')
    report('T18', 'Append data', True)

    data = cs.read('chunk_rw_test', 0, 12)
    report('T19', 'Read after append', data == 'hello world!', f'got "{data}"')

    data = cs.read('nonexistent_chunk', 0, 10)
    report('T20', 'Read empty chunk', data == '', f'got "{data}"')


def test_snapshot_copy():
    print('\n=== 3.5 Snapshot Copy ===')
    cs = get_cs(*CHUNKSERVERS[0][:2])

    cs.write('snap_src', 0, 'snapshot data')
    cs.copy_snapshot('snap_src', 'snap_dst')
    report('T21', 'Copy snapshot', True)

    data = cs.read('snap_dst', 0, 13)
    report('T22', 'Verify copy', data == 'snapshot data', f'got "{data}"')


def test_client_e2e():
    print('\n=== 3.6 Client End-to-End ===')
    from client import GFSClient

    c = GFSClient(master_host=MASTER_HOST, master_port=MASTER_PORT)
    for host, port, sid in CHUNKSERVERS:
        c.register_chunkserver(sid, host, port)

    r = c.create('/e2e.txt')
    report('T23', 'Client create', r is True)

    r = c.write('/e2e.txt', 'end to end')
    report('T24', 'Client write', r is True)

    data = c.read('/e2e.txt')
    report('T25', 'Client read', data == 'end to end', f'got "{data}"')

    r = c.append('/e2e.txt', ' test')
    report('T26', 'Client append', r is True)

    r = c.delete('/e2e.txt')
    report('T27', 'Client delete', r is True)


def test_concurrency():
    print('\n=== 3.7 Concurrency ===')
    errors = []

    def concurrent_write(file_id):
        try:
            m = get_master()
            c = get_cs(*CHUNKSERVERS[0][:2])
            fname = f'/concurrent_{file_id}.txt'
            m.create_file(fname)
            m.heartbeat('cs1', [])
            chunk = m.get_chunk_handle(fname, 0)
            c.write(chunk.chunk_handle, 0, f'data_{file_id}')
            data = c.read(chunk.chunk_handle, 0, 20)
            if f'data_{file_id}' not in data:
                errors.append(f'file {file_id}: got "{data}"')
        except Exception as e:
            errors.append(f'thread {file_id}: {e}')

    threads = [threading.Thread(target=concurrent_write, args=(i,)) for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    report('T28', 'Concurrent writes', len(errors) == 0,
           '; '.join(errors) if errors else '')


# ============ Main ============

if __name__ == '__main__':
    setup_cluster()

    test_cluster_startup()
    test_file_operations()
    test_chunk_allocation()
    test_data_read_write()
    test_snapshot_copy()
    test_client_e2e()
    test_concurrency()

    print(f'\n{"=" * 40}')
    print(f'Results: {passed} passed, {failed} failed, {passed + failed} total')
    print(f'{"=" * 40}')

    sys.exit(0 if failed == 0 else 1)
