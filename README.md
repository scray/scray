[![Build Status](https://travis-ci.org/scray/scray.svg?branch=master)](https://travis-ci.org/scray/scray)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.scray/scray/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.scray/scray-parent)

Scray - A Serving-Layer Framework for "BigData"
===============================================

A typical big-data application requires a serving layer, which will serve processed data to applications. Typically this will be either a report-generator or an interactive multi-user web-application. Interactive multi-user web-application applications usually have requirements similar to near-real-time-systems, with degrading deadlines in the order of seconds. This framework strives to support development of such applications by providing abstractions typically used in conjunction with datastores (NoSQL, as well as SQL) and lambda architectures. 

