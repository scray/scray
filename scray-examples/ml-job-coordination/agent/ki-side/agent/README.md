## Table of contents

  * [Building KI agent](#building)


### Scray KI agent


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
    ki-side-agent-0.0.1-SNAPSHOT.jar

```


# Environement types


## Modes

* Standalone
    * Set stat to `SCHEDULED`
* K8s/job
    * Configure job
    * Deploy on cluster
    * Set stat to `SCHEDULED`
* K8s/app
    * Configure job
    * Configure service for job
    * Configure ingress for service
    * Deploy on cluster
    * Set stat to `SCHEDULED`


## Setup env
### Add secret
```kubectl create secret generic data-ssh-key --from-file=id_rsa=$HOME/.ssh/id_rsa```



### Connect to Keycloak

```keytool -importcert -alias kc-crt -keystore "/c/Program Files/Eclipse Adoptium/jdk-17.0.11.9-hotspot/lib/security/cacerts" -storepass changeit -file ca.der```