package scray.loader.configuration

import scray.querying.description.TableIdentifier

trait QueryspaceOption
case class QueryspaceRowstore(table: TableIdentifier) extends QueryspaceOption
case class QueryspaceIndexstore(indextype: String, table: TableIdentifier,
    column: String, indexjobid: String, mapping: Option[String] = None) extends QueryspaceOption
case class QueryspaceMaterializedView(materializedviewjobid: String) extends QueryspaceOption