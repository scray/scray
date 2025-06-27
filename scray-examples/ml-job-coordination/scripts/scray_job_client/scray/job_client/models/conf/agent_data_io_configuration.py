from dataclasses import asdict, dataclass

@dataclass
class AgentDataIoConfiguration:

    def __init__(self, type: str, data_description: str, data: dict[str, str]) -> None:
        self.type = type
        self.data_description = data_description.replace("\"", "'")
        self._data = data