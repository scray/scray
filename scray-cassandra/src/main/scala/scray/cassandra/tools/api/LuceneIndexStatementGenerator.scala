package scray.cassandra.tools.api

import scray.querying.description.TableIdentifier
import scray.cassandra.tools.types.LuceneColumnTypes.LuceneColumnType

case class LucenIndexedColumn(name: String, dataType: LuceneColumnType)


trait LuceneIndexStatementGenerator {
  
    def getIndexString(ti: TableIdentifier, column: List[LucenIndexedColumn], luceneVersion: Tuple3[Int, Int, Int]): Option[String]
  
}