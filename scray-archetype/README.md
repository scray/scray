# Scray Maven Archetype

This archetype supports writing Scray compatible jobs by hand by creating:
- directory structure
- pom.xml that creates an ueberjar for the job
- bin directory with shell script to start the jobs
- minimal job structure

## Usage:

### Building

To use the archetype artefacts they must either be pulled from a archetype-repo or installed in the local repo.

'''mvn clean install'''

### Create project from archetype with maven
Archetypes are enhancements of maven ("plugins") that can generate new projects. To use them classical maven coordinates must be provided.

'''mvn archetype:generate -DarchetypeGroupId=scray -DarchetypeArtifactId=scray-archetype -DarchetypeVersion=0.0.1-SNAPSHOT'''

### Running the jobs:

The options '''--master''' with the Spark master URL and '''--total-executor-cores''' providing the number of cores are required by the runner script.

'''bin/submit-job.sh --master <URL> --total-executor-cores <NUMBER> <program arguments specified in the job>'''

For the url of the master there are several options:
- '''spark://<IP>:<Port>''' (while port deafaults to 7077)
- '''yarn-client''' (run a job with a local client but execute on a Hadoop yarn cluster of spark workers)
- '''yarn-cluster''' (run the client and the workers of the Spark job on a Hadoop yarn cluster)
