import json

class JobSyncApiData:
    def __init__(self):
        self.filename = None
        self.state = None
        self.dataDir = None
        self.notebookName = None
        self.state = None
        self.imageName = None
        self.processingEnv = None
        self.metadata = None

   
    @staticmethod
    def from_json(json_string):
        instance = JobSyncApiData()

        data = json.loads(json_string)
        instance.filename = data.get('filename')
        instance.state = data.get('state')
        instance.dataDir = data.get('dataDir')
        instance.notebookName = data.get('notebookName')
        instance.state = data.get('state')
        instance.imageName = data.get('imageName')
        instance.processingEnv = data.get('processingEnv')
        instance.metadata = data.get('metadata')

        return instance