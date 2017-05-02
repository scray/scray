package scray.cassandra.example

import scray.cassandra.sync.CassandraImplementation._
import scray.querying.sync.Column
import scray.querying.sync.Column
import scray.querying.sync.Columns
import scray.querying.sync.Table
import scray.querying.sync.RowWithValue
import scray.querying.sync.ColumnWithValue
import scray.querying.sync.RowWithValue

object BatchOutputTable  {
  val count = new Column[Int]("count")
  
  val columns = new Columns(new Column[String]("key") :: count :: Nil, "(key)", None)
  val table = new Table("\"BDQ_BATCH\"", "\"BatchCountExample\"", columns)
    
  val row = new RowWithValue(new ColumnWithValue[String]("key", "key") :: new ColumnWithValue("count", 1) :: Nil, "(key)", None)
    
  def setCounter(count: Int) = {new RowWithValue(new ColumnWithValue[String]("key", "key") :: new ColumnWithValue("count", count) :: Nil, "(key)", None)}
}