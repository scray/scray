package scray.loader.tools.api

import scray.querying.description.TableIdentifier

trait CassandraIndexStatementGenerator {
  def getIndexString(ti: TableIdentifier, column: List[String]): String
}