package scray.cassandra.tools

import scray.querying.description.TableIdentifier
import scray.cassandra.tools.api.LuceneIndexStatementGenerator
import scray.cassandra.tools.api.LucenIndexedColumn
import com.typesafe.scalalogging.slf4j.LazyLogging

/**
 * Create CQL statement to index columns with "Stratio’s Cassandra Lucene Index"
 */
class CassandraLuceneIndexStatementGeneratorImpl extends LuceneIndexStatementGenerator with LazyLogging {
  
  def getAlterTableStatement(ti: TableIdentifier): String = {
    s"""ALTER TABLE "${ti.dbId}"."${ti.tableId}" ADD lucene text;"""
  }
  
  def getIndexString(ti: TableIdentifier, column: List[LucenIndexedColumn], luceneVersion: Tuple3[Int, Int, Int]): Option[String] = {
    
   if(luceneVersion == (2,2,7)) {
     Some(getIndexStringLucene2d2d3(ti, column, luceneVersion))
   } else {
     logger.error(s"No generator for lucene version ${luceneVersion}")
     None
   }
  }
  
  val ceateFileElement = (acc: String, column: LucenIndexedColumn) => {
     if (acc.endsWith("{")) { // Detect first element
        s"""${acc} 
                  ${column.name} : {type: "${column.dataType}"}"""
      } else {
        s"""${acc} 
                  ${column.name} : {type: "${column.dataType}"}"""
      }
    }
  
  private def getIndexStringLucene2d2d3(ti: TableIdentifier, columns: List[LucenIndexedColumn], luceneVersion: Tuple3[Int, Int, Int]): String = {

    val statementHeader = "CREATE CUSTOM INDEX \"" + ti.tableId + "_lucene_index\" ON \"" + ti.dbId + "\".\"" + ti.tableId + "\" (lucene) \n" +
    "USING 'com.stratio.cassandra.lucene.Index' \n" +
    "WITH OPTIONS = { \n" +
       "\t'refresh_seconds' : '1', \n" +
       "\t\'schema\' : '{ \n" +
          "\t\tfields : {"
      val statementEnd ="\n\t\t}\n" +
         "\t}' \n" + 
     "};"
         
   columns.foldLeft(statementHeader)(ceateFileElement) + statementEnd
  }
  
  def getLuceneType[T](data: T): String = {
    data match {
      case _: String => "string"
      case _: Long => "long"
      case _: Int => "integer"
      case _: Boolean => "boolean"
    }
  }

}