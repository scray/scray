## Table of contents

  * [Building  OCI build agent](#building)


### Scray OCI build agent.

Agent to crate a OCI image based on a Dockerfile.



### Prerequisites
* Java 17
* Maven (tested with 3.8)


### Building

To build the core project, do the following:

```
    mvn clean package
```

### Starting

```
    oci-image-builder-0.0.1-SNAPSHOT.jar

```

## Setup env

 * Datashare access key
    ```
        ~/.ssh/id_rsa
    ```
 * Docker command.

## Client requrements
### Job env
    http://research.dev.example.com/oci/image/
### Requirements
    * Job with a Dockerfile