from dataclasses import asdict


class AgentDataIoConfiguration:

    def __init__(self, type: str, data_description: str, data: dict[str, str]) -> None:
        self.type = type
        self.data_description = data_description
        self._data = data
    
    def to_dict(self):
        return asdict(self)
