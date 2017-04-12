package scray.cassandra.tools.api

import scray.querying.description.TableIdentifier

case class Column(name: String, dataType: String)


trait LuceneIndexStatementGenerator {
  
    def getIndexString(ti: TableIdentifier, column: List[Column], luceneVersion: Tuple3[Int, Int, Int]): Option[String]
  
}