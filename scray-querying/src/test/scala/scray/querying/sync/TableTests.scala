package scray.querying.sync

import org.junit.runner.RunWith
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner

import scray.querying.sync.cassandra.CassandraImplementation.genericCassandraColumnImplicit
import scray.querying.sync.types.Column
import scray.querying.sync.types.ColumnWithValue
import scray.querying.sync.types.Columns
import scray.querying.sync.types.RowWithValue
import scray.querying.sync.types.SyncTableBasicClasses.SyncTableRowEmpty
import scray.querying.sync.types.Table


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
      val c2 = c1.copy()

      assert(c2.name === "c1")
      assert(c2.value === "v1")
      assert(c2.getDBType === "text") 
    }
    " test foldLeft on rows " in {
        val columns = new ColumnWithValue[Int]("c1", 1) ::
                      new ColumnWithValue[String]("c2", "2") ::
                      new ColumnWithValue[Boolean]("c3", true) :: Nil

      val row1 = new RowWithValue(columns, "p1", None)

      val namesAsString = row1.foldLeft("")((acc, column) => acc + column.name)
      val valuesAsString = row1.foldLeft("")((acc, column) => acc + column.value)

      assert(namesAsString === "c1c2c3")
      assert(valuesAsString === "12true")
    }
    " test db type detection in tables " in {

      val s = SyncTableRowEmpty
      assert(s.indexes.get.head === "locked")
      assert(s.columns.head.getDBType === "text")
    }
    " clone rows " in {
      
      val c1 = new ColumnWithValue[Int]("c1", 1)
      val c2 = new ColumnWithValue[String]("c2", "2")
      val c3 = new ColumnWithValue[Boolean]("c3", true)
         
      val columns = c1 :: c2 :: c3 :: Nil
          
      val row1 = new RowWithValue(columns, "p1", None)
      
      val row2 = row1.copy()
      row2.columns.map { x =>  
        x.value match {
          case s: String => 
            x.asInstanceOf[ColumnWithValue[String]].setValue("3")
          case i: Int =>
             x.asInstanceOf[ColumnWithValue[Int]].setValue(2)
          case i: Boolean =>
             x.asInstanceOf[ColumnWithValue[Boolean]].setValue(false) 
        }
      }

      // Check if new values exists
      row2.columns.map { x =>  
        x.value match {
          case s: String => 
            assert(x.asInstanceOf[ColumnWithValue[String]].value === "3")
          case i: Int =>
             assert(x.asInstanceOf[ColumnWithValue[Int]].value === 2)
          case b: Boolean =>
             assert(x.asInstanceOf[ColumnWithValue[Boolean]].value === false) 
        }
      }
      
      // Check if old values still exists
      row1.columns.map { x =>  
        x.value match {
          case s: String => 
            assert(x.asInstanceOf[ColumnWithValue[String]].value === "2")
          case i: Int =>
             assert(x.asInstanceOf[ColumnWithValue[Int]].value === 1)
          case b: Boolean =>
             assert(x.asInstanceOf[ColumnWithValue[Boolean]].value === true) 
        }
      }
    }
    
    " add multiple rows " in {
      val columns = new Column[String]("a") :: new Column[String]("b")  :: new Column[String]("c") :: Nil
      val columnsObject  = new Columns(columns, "", None)
      
      val table =  new Table("ks1", "tn1", columnsObject)
      
      val newData1 = new ColumnWithValue[String]("c2", "2") :: new ColumnWithValue[String]("c2", "2") :: new ColumnWithValue[String]("c2", "2") :: Nil
      val newData2 = new ColumnWithValue[String]("c2", "3") :: new ColumnWithValue[String]("c2", "2") :: new ColumnWithValue[String]("c2", "3") :: Nil
      val newData3 = new ColumnWithValue[String]("c2", "4") :: new ColumnWithValue[String]("c2", "2") :: new ColumnWithValue[String]("c2", "4") :: Nil

      table.addRow(new RowWithValue(newData1, "", None))
      table.addRow(new RowWithValue(newData1, "", None))
      table.addRow(new RowWithValue(newData2, "", None))
      table.addRow(new RowWithValue(newData3, "", None))
      
      assert(table.getRows.head.columns.head.value === "2")
      assert(table.getRows.tail.head.columns.head.value === "2")
      assert(table.getRows.tail.tail.head.columns.head.value === "3")
      assert(table.getRows.tail.tail.tail.head.columns.head.value === "4")
    }
  }

}