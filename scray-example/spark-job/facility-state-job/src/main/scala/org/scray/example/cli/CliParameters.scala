package org.scray.example.cli

case class CliParameters(
  var batch: Boolean = false,
  var sparkMaster: String = "spark://127.0.0.1:7077",
  var confFilePath: Option[String] = None,
  var cassandraSeed: String = "db-cassandra"
)