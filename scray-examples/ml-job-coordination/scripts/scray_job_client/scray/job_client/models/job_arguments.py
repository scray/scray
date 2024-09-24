import json

class JobArguments:
    def __init__(self):
        self.context = None,
        self.job_name = None
        self.source_data = None
        self.notebook_name = None
        self.initial_state = None
        self.processing_env = None
        self.docker_image = None
        self.job_name_literally = None
        self.state = None
        self.sft_user = "ubuntu"
        self.sft_host = "ml-integration.research.dev.example.com"

  