import threading


class DataQueue:
    """Write pipeline data queue - buffers packets before sending to chunkservers."""

    def __init__(self):
        self.packets = []
        self.lock = threading.Lock()

    def enqueue(self, packet):
        with self.lock:
            self.packets.append(packet)

    def dequeue(self):
        with self.lock:
            if self.packets:
                return self.packets.pop(0)
            return None

    def peek(self):
        with self.lock:
            return self.packets[0] if self.packets else None

    def is_empty(self):
        with self.lock:
            return len(self.packets) == 0

    def size(self):
        with self.lock:
            return len(self.packets)

    def clear(self):
        with self.lock:
            self.packets.clear()

    def get_all(self):
        with self.lock:
            items = list(self.packets)
            self.packets.clear()
            return items


class ACKQueue:
    """Tracks acknowledgments from chunkservers in the write pipeline."""

    def __init__(self):
        self.acked = {}       # packet_id -> set of server_ids that acked
        self.expected = {}    # packet_id -> set of server_ids expected
        self.lock = threading.Lock()

    def expect(self, packet_id, server_ids):
        with self.lock:
            self.expected[packet_id] = set(server_ids)
            self.acked[packet_id] = set()

    def ack(self, packet_id, server_id):
        with self.lock:
            if packet_id in self.acked:
                self.acked[packet_id].add(server_id)

    def is_complete(self, packet_id):
        with self.lock:
            if packet_id not in self.expected:
                return False
            return self.acked[packet_id] >= self.expected[packet_id]

    def get_failed_servers(self, packet_id):
        with self.lock:
            if packet_id not in self.expected:
                return set()
            return self.expected[packet_id] - self.acked.get(packet_id, set())

    def remove(self, packet_id):
        with self.lock:
            self.expected.pop(packet_id, None)
            self.acked.pop(packet_id, None)

    def clear(self):
        with self.lock:
            self.acked.clear()
            self.expected.clear()
