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
import scray.querying.sync.types.ColumnInt
import scray.querying.sync.types.ColumnString
import scray.querying.sync.types.ColumnLong
import scray.querying.sync.types.ColumnBoolean
import scray.querying.sync.types.SyncTableBasicClasses.DbTypeMapping

@RunWith(classOf[JUnitRunner])
class SyncTableTests extends WordSpec {
  // Example db type mapper
  object dbTypes extends DbTypeMapping {
    def getDbType(column: Column): String = { "" }
    def getDbType(column: ColumnInt): String = { "" }
    def getDbType(column: ColumnLong): String = { "" }
    def getDbType(column: ColumnString): String = { "" }
    def getDbType(column: ColumnBoolean): String = { "" }
  }
  
  "Tables " should {
    " return DB type" in {
       val c1 = new ColumnString("c1", dbTypes)
       
       assert(c1.name === "c1")
       assert(c1.getDBType === "String")
    }
    " set and get values " in {
      val c1 = new ColumnWithValue[Boolean, String]("c1", dbTypes, "v1")
      
       assert(c1.name === "c1")
       assert(c1.value === "v1")
       assert(c1.getDBType === "DB_TYPE_1")
    }
    " test foldLeft on rows " in {
      val columns = new ColumnWithValue[Boolean, Int]("c1", dbTypes, 1) :: 
                    new ColumnWithValue[Boolean, String]("c2", dbTypes, "2") :: 
                    new ColumnWithValue[Boolean, Boolean]("c3", dbTypes, true) :: Nil

      val row1 = new RowWithValue(columns, "p1", Some(columns))
      
      val namesAsString =  row1.foldLeft("")((acc, column) => acc + column.name)
      val valuesAsString = row1.foldLeft("")((acc, column) => acc + column.value)
      
      assert(namesAsString === "c1c2c3")
      assert(valuesAsString === "12true")
    }
    " generate a table " in {
      
        val syncTable = new SyncTableRowEmpty(dbTypes)

      
    }
  }
  
}