from scray.job_client.models.conf.agent_data_io_configuration import AgentDataIoConfiguration


class S3Configuration(AgentDataIoConfiguration):
    def __init__(self, data: dict[str, str]) -> None:
       self.type = "http://scray.org/agent/conf/io/type/s3"
       self.hostname = data["hostname"]  
       self.bucket = data["bucket"]
       self.path = data["path"]

    def __init__(self, hostname: str, bucket: str, path: str, data_description: str = None) -> None:
       self.type = "http://scray.org/agent/conf/io/type/s3"
       self.hostname = hostname
       self.bucket = bucket
       self.path = path
       self.data_description = data_description
        