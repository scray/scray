package scray.cassandra.tools.api

import scray.cassandra.tools.types.LuceneColumnTypes.LuceneColumnType
import scray.querying.description.TableIdentifier

case class LucenIndexedColumn(name: String, dataType: LuceneColumnType, isSorted: Boolean)


trait LuceneIndexStatementGenerator {
  
    def getIndexString(ti: TableIdentifier, column: List[LucenIndexedColumn], luceneVersion: Tuple3[Int, Int, Int]): Option[String]
  
}