package scray.cassandra.tools.types

import scray.cassandra.tools.types.CassandraColumnTypeReader

object Main {
  def main(args: Array[String]) = {
    val cassandra =  new CassandraColumnTypeReader("10.11.22.31", "umcdev")
    
    val casType = cassandra.getType("\"SilError\"", "key")
    val luceneType = LuceneColumnTypes.getLuceneType(casType)
    
    println(casType)
    println(luceneType)
    
    
  }
}