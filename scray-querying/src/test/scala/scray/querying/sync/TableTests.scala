package scray.querying.sync

import scala.annotation.tailrec
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfter
import org.scalatest.WordSpec
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner
import scray.querying.description.Row
import scray.querying.sync.types.Column
import scray.querying.sync.types.ColumnWithValue
import scray.querying.sync.types.DbSession
import scray.querying.sync.types.RowWithValue
import scray.querying.sync.types.SyncTableBasicClasses.SyncTableRowEmpty
import scray.querying.sync.types.Table
import scray.querying.sync.cassandra.CassandraImplementation._

@RunWith(classOf[JUnitRunner])
class SyncTableTests extends WordSpec {
  
  
  "Tables " should {
    " return DB type" in {
       val c1 = new Column[String]("c1")
       
       assert(c1.name === "c1")
       assert(c1.getDBType === "text")
    }
    " set and get values " in {
      val c1 = new ColumnWithValue[String]("c1", "v1")
      
       assert(c1.name === "c1")
       assert(c1.value === "v1")
       assert(c1.getDBType === "text")
    }
    " test foldLeft on rows " in {
      val columns = new ColumnWithValue[Int]("c1", 1) :: 
                    new ColumnWithValue[String]("c2", "2") :: 
                    new ColumnWithValue[Boolean]("c3", true) :: Nil

      val row1 = new RowWithValue(columns, "p1", None)
      
      val namesAsString =  row1.foldLeft("")((acc, column) => acc + column.name)
      val valuesAsString = row1.foldLeft("")((acc, column) => acc + column.value)
      
      assert(namesAsString === "c1c2c3")
      assert(valuesAsString === "12true")
    }
    " test db type detection in tables " in { 
      
      val s = new SyncTableRowEmpty()
      assert(s.indexes.get.head === "locked")
      assert(s.columns.head.getDBType === "text")
    }
  }
  
}