import os
import xml.etree.ElementTree as ET

_CONFIG_DIR = os.path.join(os.path.dirname(__file__), '..', 'config')


class GFSConfig:
    """Parses Hadoop-style XML configuration files for mGFS."""

    DEFAULTS = {
        'gfs.master.host': '127.0.0.1',
        'gfs.master.port': '9090',
        'gfs.chunk.size': '67108864',
        'gfs.replication': '3',
        'gfs.lease.duration': '60',
        'gfs.heartbeat.interval': '5',
        'gfs.data.dir': './data',
    }

    def __init__(self, config_dir=None):
        self.config_dir = os.path.abspath(config_dir or _CONFIG_DIR)
        self.properties = dict(self.DEFAULTS)
        self._load_site_xml()

    def _load_site_xml(self):
        site_path = os.path.join(self.config_dir, 'gfs-site.xml')
        if not os.path.exists(site_path):
            return
        tree = ET.parse(site_path)
        root = tree.getroot()
        for prop in root.findall('property'):
            name = prop.find('name').text
            value = prop.find('value').text
            if name and value is not None:
                self.properties[name] = value

    def get(self, key, default=None):
        return self.properties.get(key, default)

    def get_int(self, key, default=0):
        return int(self.properties.get(key, default))

    def get_master_address(self):
        host = self.get('gfs.master.host')
        port = self.get_int('gfs.master.port')
        return host, port

    def get_chunkservers(self):
        """Parse chunkservers file, returns list of (host, port, server_id)."""
        cs_path = os.path.join(self.config_dir, 'chunkservers')
        servers = []
        if not os.path.exists(cs_path):
            return servers
        with open(cs_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                parts = line.split()
                addr = parts[0]
                server_id = parts[1] if len(parts) > 1 else addr
                host, port = addr.split(':')
                servers.append((host, int(port), server_id))
        return servers
