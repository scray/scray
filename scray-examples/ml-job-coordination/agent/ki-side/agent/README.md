## Table of contents

  * [Building FMU-Management](#building-fmu-management)


### Building FMU-Management

To properly build the FMU-Management, the following tools should be installed:

### Prerequisites
* Java 11 JDK
* Maven (tested with 3.8)


### Building

To build the core project, do the following:

```
    mvn clean package
```

### Starting

```
    java -jar target\fmu-management-0.0.1-SNAPSHOT.jar
```