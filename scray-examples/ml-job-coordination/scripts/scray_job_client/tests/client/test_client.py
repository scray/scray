#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from unittest import TestCase
import unittest
import logging
from scray.client.client import ScrayClient

from scray.client.config import ScrayClientConfig
from scray.job_client.client import ScrayJobClient
from scray.job_client.models.agent_configuration import AgentConfiguration
from scray.job_client.models.conf.s3_configuration import S3Configuration
from scray.job_client.models.conf.scray_job_metadata_configuration import ScrayJobMetadataConfiguration
from scray.job_client.models.job_state_configuration import JobStates

logger = logging.getLogger(__name__)

class TestScrayClient(TestCase):
    def test_client(self):
        
        config = ScrayClientConfig(
            host_address = "scray.example.com",
            port = 8082
        )

        client = ScrayClient(client_config=config)
        version = client.getLatestVersion


    def test_agent_conf_loading(self):
        config = ScrayClientConfig(
            host_address = "http://localhost",
            port = 8082
        )

        client = ScrayJobClient(config=config)


        description = """
        {
            "openapi": "3.1.0",
            "info": {
                "title": "FastAPI",
                "version": "0.1.0"
            },
            "paths": {
                "/jira/servicedesk/comment/": {
                    "put": {
                        "summary": "Add Comment",
                        "description": "Creates an internal comment on an existing customer request.",
                        "operationId": "add_comment_jira_servicedesk_comment__put",
                        "requestBody": {
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "$ref": "#/components/schemas/JiraCommentDetails"
                                    }
                                }
                            },
                            "required": true
                        },
                        "responses": {
                            "200": {
                                "description": "Successful Response",
                                "content": {
                                    "application/json": {
                                        "schema": {}
                                    }
                                }
                            },
                            "422": {
                                "description": "Validation Error",
                                "content": {
                                    "application/json": {
                                        "schema": {
                                            "$ref": "#/components/schemas/HTTPValidationError"
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "components": {
                "schemas": {
                    "HTTPValidationError": {
                        "properties": {
                            "detail": {
                                "items": {
                                    "$ref": "#/components/schemas/ValidationError"
                                },
                                "type": "array",
                                "title": "Detail"
                            }
                        },
                        "type": "object",
                        "title": "HTTPValidationError"
                    },
                    "JiraCommentDetails": {
                        "properties": {
                            "issueId": {
                                "type": "string",
                                "title": "Issueid"
                            },
                            "comment": {
                                "type": "string",
                                "title": "Comment"
                            }
                        },
                        "type": "object",
                        "required": [
                            "issueId",
                            "comment"
                        ],
                        "title": "JiraCommentDetails"
                    },
                    "ValidationError": {
                        "properties": {
                            "loc": {
                                "items": {
                                    "anyOf": [
                                        {
                                            "type": "string"
                                        },
                                        {
                                            "type": "integer"
                                        }
                                    ]
                                },
                                "type": "array",
                                "title": "Location"
                            },
                            "msg": {
                                "type": "string",
                                "title": "Message"
                            },
                            "type": {
                                "type": "string",
                                "title": "Error Type"
                            }
                        },
                        "type": "object",
                        "required": [
                            "loc",
                            "msg",
                            "type"
                        ],
                        "title": "ValidationError"
                    }
                }
            }
        }"""

  
        # Create configuration
        name = "agent-007"
        env = "http://scray.org/sync/agent/configuration"

        states = [JobStates(
                env="http://scray.org/ai/jobs/env/see/000", 
                trigger_states=["UPLOADED"], 
                error_states=["CONVERSION_ERROR"], 
                completed_states=["SUMMARIZED"]
            )]
        
        input  = S3Configuration(
                hostname = "https://s3.example.com", 
                bucket = "data-bucket", 
                path = "/in-data/data.txt", 
                data_description = description
            )
        
        output = ScrayJobMetadataConfiguration(
                hostname = "http://ml-integration.research.example.com", 
                env = "http://scray.org/ai/jobs/env/see/000/result", 
                jobname= "job512", 
                data_description = ""
            )

        agent_conf = AgentConfiguration(
            env=env, name=name, 
            job_states=states, 
            data_input_conf=input, 
            data_output_conf=output
            )


        # Set new configuration
        client.set_agent_conf(env = "env1", agent_name = "process", configuration = agent_conf)

        # Read configuration
        agent_conf_read = client.get_agent_conf(env = "env1", agent_name = "process")

        # Print configuration
        print("Agent name: " + agent_conf_read.name)

        input_conf = agent_conf_read.data_input_conf
        output_conf = agent_conf_read.data_output_conf

        if isinstance(input_conf, S3Configuration):
            print("Instance is of S3Configuration")
            print("Host: ", input_conf.hostname)
            print("Bucket: ", input_conf.bucket)
            print("Path: ", input_conf.path)
        
        if isinstance(output_conf, ScrayJobMetadataConfiguration):
            print("Instance is of ScrayJobMetadataConfiguration")
            print("Host: ", output_conf.hostname)
            print("Name: ", output_conf.jobname)
            print("Env: ",  output_conf.env)

if __name__ == '__main__':
    unittest.main()