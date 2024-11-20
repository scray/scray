from dataclasses import asdict, dataclass
import json
from scray.job_client.models.conf.agent_data_io_configuration import AgentDataIoConfiguration

@dataclass
class ScrayJobMetadataConfiguration(AgentDataIoConfiguration):
    
    type: str
    hostname: str
    env: str
    jobname: str
    data_description: str 

    @staticmethod
    def from_dict(data: dict[str, str]) -> None:
      return ScrayJobMetadataConfiguration(
         hostname = data["hostname"],  
         env = data["env"],
         jobname = data["jobname"],
         data_description = data["data_description"]
      )

    def __init__(self, hostname: str, env: str, jobname: str, data_description: str = None) -> None:
       self.type = "http://scray.org/agent/conf/io/type/scrayjobmetadata"
       self.hostname = hostname
       self.env = env
       self.jobname = jobname
       self.data_description = data_description
   
    def to_dict(self):
        return asdict(self)
    
        