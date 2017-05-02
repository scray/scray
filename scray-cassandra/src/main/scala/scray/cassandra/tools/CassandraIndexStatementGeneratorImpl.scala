package scray.cassandra.tools

import scray.cassandra.tools.api.CassandraIndexStatementGenerator
import scray.querying.description.TableIdentifier

class CassandraIndexStatementGeneratorImpl extends CassandraIndexStatementGenerator {

  /**
   * Create cassandra index statement
   * All elements are case sensitive.
   */
  def getIndexString(ti: TableIdentifier, column: List[String]): String = {

    val concatColumns = (acc: String, column: String) => {
      if (acc.endsWith("(")) {
        acc + "\"" + column + "\""
      } else {
        acc + ", \"" + column + "\""
      }
    }

    val indexString = s"""CREATE INDEX ON "${ti.dbId}"."${ti.tableId}" ${column.foldLeft("(")(concatColumns)} );"""

    indexString
  }
}