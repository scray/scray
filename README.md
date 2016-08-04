Scray - A Serving-Layer Framework for "BigData"
===============================================

A typical big-data application requires a serving layer, which will serve processed data to applications. Typically this will be either a report-generator or an interactive multi-user web-application. Interactive multi-user web-application applications usually have requirements similar to near-real-time-systems, with degrading deadlines in the order of seconds. This framework strives to support development of such applications by providing abstractions typically used in conjunction with datastores (NoSQL, as well as SQL) and lambda architectures. 

Assumptions
-----------




Compiled Querys
---------------
A compiled query is an artifact that allows to specify a specific type of query. It could be called a namespace for
queries. A compiled query has:
* a hard-coded name to identify it, called "Queryspace-name".
* a set of dbms tables compiled in which may be queried. This is includes the hand-provided indexing tables.
* its indexing information compiled in, which may then be used by the planner to plan the query execution.

Registry
--------
The registry is used to look up tables, query spaces and columns. Each query space must register itself as well as its columns and tables to make it available to the planner.

Prerequisites
-------------

To build Scray Maven needs a lot of heap and loads a lot of classes. To provide more RAM Maven can be configured by an environment variable called `MAVEN_OPTS`, e.g. by `export MAVEN_OPTS='-Xmx1024m -XX:MaxPermSize=256m'`.

Development
-----------

* Download sources: `git clone https://github.com/scray-bdq/scray.git`
* Build using maven: `mvn clean install -DskipTests`
* Use Scala-Ide Import->Existing Maven Projects and point Eclipse to your scray folder
* Make all Scala-Projects based on Scala 2.10: Project Preferences->Scala Compiler->Use Project Settings->Latest 2.10
* Scray-Java: Import `scray-java/target/generated-sources/thrift/scrooge` as an additional source folder





