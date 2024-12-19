from dataclasses import asdict, dataclass
import json
from scray.job_client.models.conf.agent_data_io_configuration import AgentDataIoConfiguration

@dataclass
class S3Configuration(AgentDataIoConfiguration):
    
    type: str
    hostname: str
    bucket: str
    path: str
    data_description: str 

    @staticmethod
    def from_dict(data: dict[str, str]) -> None:
      return S3Configuration(
         hostname = data["hostname"],  
         bucket = data["bucket"],
         path = data["path"],
         data_description = data["data_description"]
      )

    def __init__(self, hostname: str, bucket: str, path: str, data_description: str = None) -> None:
       self.type = "http://scray.org/agent/conf/io/type/s3"
       self.hostname = hostname
       self.bucket = bucket
       self.path = path
       self.data_description = data_description
   
    def to_dict(self):
        return asdict(self)
    
        