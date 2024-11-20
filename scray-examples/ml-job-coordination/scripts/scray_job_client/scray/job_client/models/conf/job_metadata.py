from scray.job_client.models.conf.agent_data_io_configuration import AgentDataIoConfiguration


class JobMetadataConfig(AgentDataIoConfiguration):
    def __init__(self, type: str, data: dict[str, str]) -> None:
       self.type = type
       self.hostname = data["hostname"]  
       self.bucket = data["bucket"]
       self.path = data["path"]