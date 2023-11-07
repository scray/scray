from dataclasses import dataclass
from pprint import pformat
from pprint import pprint

@dataclass
class VersionedData(object):

    def __init__(self, data_source=None, merge_key=None, version=None, data=None, version_key=None): 
        self._data_source = None
        self._merge_key = None
        self._version = None
        self._data = None
        self._version_key = None
        self.discriminator = None
        if data_source is not None:
            self.data_source = data_source
        if merge_key is not None:
            self.merge_key = merge_key
        if version is not None:
            self.version = version
        if data is not None:
            self.data = data
        if version_key is not None:
            self.version_key = version_key


    def __init__(self, dic):
        self._data_source = dic["dataSource"]
        self._merge_key = dic["mergeKey"]
        self._data = dic["data"]
        self._version = dic["version"]
        self._version_key = dic["versionKey"]

    @property
    def data_source(self):
        return self._data_source

    @data_source.setter
    def data_source(self, data_source):
        self._data_source = data_source

    @property
    def merge_key(self):
        return self._merge_key

    @merge_key.setter
    def merge_key(self, merge_key):
        self._merge_key = merge_key

    @property
    def version(self):
        return self._version

    @version.setter
    def version(self, version):
        self._version = version

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, data):
        self._data = data

    @property
    def version_key(self):
        return self._version_key

    @version_key.setter
    def version_key(self, version_key):
        self._version_key = version_key


    def to_str(self):
        return "{}, {}, {}, {}, {}".format(
        self.data_source,
        self.merge_key,
        self.version,
        self.data,
        self.version_key)

    def __repr__(self):
        return self.to_str()

    def __eq__(self, other):
        if not isinstance(other, VersionedData):
            return False
        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not self == other
    
    
    def to_api_json(self) -> str:
        data = self.data.replace("\"", "\\\"")
        return f"{{\"dataSource\": \"{self.data_source}\", \"mergeKey\": \"{self.merge_key}\", \"version\": {self.version}, \"data\": \"{data}\", \"versionKey\": {self.version_key}}}"
    
