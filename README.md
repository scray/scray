[![Stories in Ready](https://badge.waffle.io/scray-bdq/scray.png?label=ready&title=Ready)](https://waffle.io/scray-bdq/scray)
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

