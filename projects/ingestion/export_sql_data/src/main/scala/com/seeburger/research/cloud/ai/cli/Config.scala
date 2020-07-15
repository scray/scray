package com.seeburger.research.cloud.ai.cli

/**
 * container class for command line arguments. Change this along with the command line parser.
 */
case class Config(
  master: String,                             // Spark master URL
  batch: Boolean = false,
  sqlUser: String = "sqlUser42",
  sqlPassword: String = "pw42"
)