package scray.querying.sync.types

import scala.reflect.runtime.universe._
import shapeless._
import com.datastax.driver.core.Statement
import scala.collection.JavaConverters._
import com.datastax.driver.core.querybuilder.Insert
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.websudos.phantom.CassandraPrimitive._
import com.websudos.phantom.CassandraPrimitive
import java.sql.Struct
import java.sql.RowId
import java.sql.Clob
import java.net.URL
import java.math.BigInteger
import java.sql.NClob
import java.sql.Blob
import java.sql.SQLXML
import java.sql.Timestamp
import java.sql.Time
import java.sql.Ref
import scala.reflect.ClassTag
import scray.querying.sync.cassandra.CassandraImplementation._
import scala.collection.mutable.ListBuffer

class Table[T <: AbstractRows](val keySpace: String, val tableName: String, val columns: T) {
  val rows: ListBuffer[RowWithValue]= ListBuffer[RowWithValue]()
  
  def addRow(row: RowWithValue) {
    rows += row
  }
  
  def getRows = {
    rows
  }
}

trait DBColumnImplementation[T] {
  def getDBType: String
  def fromDBType(value: AnyRef): T
  def toDBType(value: T): AnyRef
}

class Column[T: DBColumnImplementation](val name: String) { self =>
  val dbimpl = implicitly[DBColumnImplementation[T]]

  // type DB_TYPE
  def getDBType: String = dbimpl.getDBType
}

abstract class AbstractRows {
  type ColumnType <: Column[_]
  val columns: List[ColumnType]
  val primaryKey = ""
  val indexes: Option[List[String]] = None

  def foldLeft[B](z: B)(f: (B, ColumnType) => B): B = {
    columns.foldLeft(z)(f)
  }
}

abstract class ArbitrarylyTypedRows extends AbstractRows {
  override type ColumnType = Column[_]
}

case class ColumnWithValue[ColumnT: DBColumnImplementation](override val name: String, var value: ColumnT) extends Column[ColumnT](name) {
  def setValue(a: ColumnT): Unit = { value = a }
  override def clone(): ColumnWithValue[ColumnT] = {
    new ColumnWithValue(this.name, this.value)
  }
}

class RowWithValue(
    override val columns: List[ColumnWithValue[_]],
    override val primaryKey: String,
    override val indexes: Option[List[String]]) extends AbstractRows {

  override type ColumnType = ColumnWithValue[_]

  def copy(): RowWithValue = {
    val columnCopies = this.columns.foldLeft(List[ColumnWithValue[_]]())((acc, column) => { column.clone() :: acc })
    new RowWithValue(columnCopies, this.primaryKey, this.indexes)
  }
}

class Columns(
  override val columns: List[Column[_]],
  override val primaryKey: String,
  override val indexes: Option[List[String]]) extends AbstractRows {
  
  override type ColumnType = Column[_]
}

abstract class DbSession[Statement, InsertIn, Result](val dbHostname: String) {
  def execute(statement: Statement): Result
  def execute(statement: String): Result
  def insert(statement: InsertIn): Result
}

object SyncTable {
  def apply(keySpace: String, tableName: String) = {
    new Table(keySpace, tableName, new SyncTableBasicClasses.SyncTableRowEmpty)
  }
}

object VoidTable {
  def apply(keySpace: String, tableName: String, columns: AbstractRows) = {
    new Table(keySpace, tableName, columns)
  }
}
object DataTable {
  def apply(keySpace: String, tableName: String, columns: RowWithValue) = {
    new Table(keySpace, tableName, columns)
  }
}

object SyncTableBasicClasses {

  class SyncTableRowEmpty() extends ArbitrarylyTypedRows {

    val jobname = new Column[String]("jobname")
    val versionNr = new Column[Int]("versionNr")
    val creationTime = new Column[Long]("creationTime")
    val batcheVersions = new Column[Int]("batcheVersion")
    val onlineVersions = new Column[Int]("onlineVersions")
    val tablename = new Column[String]("tablename")
    val locked = new Column[Boolean]("locked")
    val online = new Column[Boolean]("online")
    val completed = new Column[Boolean]("completed")
    val state = new Column[String]("state")

    override val columns = jobname :: creationTime :: versionNr :: batcheVersions :: onlineVersions :: tablename :: locked :: online :: completed :: state :: Nil
    override val primaryKey = s"(${jobname.name}, ${online.name}, ${versionNr.name})"
    override val indexes: Option[List[String]] = Option(List(locked.name))
  }
}

//object SyncTableInitFunction {


//class SyncTableColumnsE() extends SyncTableColumns[Column[_]] {
//  override val jobname = new Column[String]("jobname", CassandraTypeName.getCassandraTypeName(_))
//  override val time = new Column[Long]("time", CassandraTypeName.getCassandraTypeName(_))
//  override val lock = new Column[Boolean]("lock", CassandraTypeName.getCassandraTypeName(_))
//  override val online = new Column[Boolean]("online", CassandraTypeName.getCassandraTypeName(_))
//  override val nr = new Column[Int]("nr", CassandraTypeName.getCassandraTypeName(_))
//  override val batches = new Column[Int]("batches", CassandraTypeName.getCassandraTypeName(_))
//  override val onlineVersions = new Column[Int]("onlineVersions", CassandraTypeName.getCassandraTypeName(_))
//  override val tablename = new Column[String]("tablename", CassandraTypeName.getCassandraTypeName(_))
//  override val completed = new Column[Boolean]("completed", CassandraTypeName.getCassandraTypeName(_))
//  override val state =  new Column[String]("state", CassandraTypeName.getCassandraTypeName(_))
//}
//class SyncTableColumnsValuesS[ColumnValueT, ColumnT <: Column[_]](initFunction: (ColumnT)=> ColumnT, nrV: IndexedSeq[Int], jobnameV: IndexedSeq[String], timeV: IndexedSeq[Long], lockV: IndexedSeq[Boolean], onlineV: IndexedSeq[Boolean], completedV: IndexedSeq[Boolean], tablenameV: IndexedSeq[String], batchesV: IndexedSeq[Int], onlineVersionsV: IndexedSeq[Int], stateV: IndexedSeq[String]) extends SyncTableColumns[ColumnV[ColumnValueT]] {
//
//  override val jobname: ColumnT = initFunction.apply(jobname)
//  override val time = new ColumnV[IndexedSeq[Long]]("time", CassandraTypeName.getCassandraTypeName, timeV)
//  override val lock = new ColumnV[IndexedSeq[Boolean]]("lock", CassandraTypeName.getCassandraTypeName, lockV)
//  override val online = new ColumnV[IndexedSeq[Boolean]]("online", CassandraTypeName.getCassandraTypeName, onlineV)
//  override val nr = new ColumnV[IndexedSeq[Int]]("nr", CassandraTypeName.getCassandraTypeName, nrV)
//  override val batches = new ColumnV[IndexedSeq[Int]]("batches", CassandraTypeName.getCassandraTypeName, batchesV)
//  override val onlineVersions = new ColumnV[IndexedSeq[Int]]("onlineVersions", CassandraTypeName.getCassandraTypeName, onlineVersionsV)
//  override val tablename = new ColumnV[IndexedSeq[String]]("tablename", CassandraTypeName.getCassandraTypeName, tablenameV)
//  override val completed = new ColumnV[IndexedSeq[Boolean]]("completed", CassandraTypeName.getCassandraTypeName, completedV)
//  override val state =  new ColumnV[IndexedSeq[String]]("state", CassandraTypeName.getCassandraTypeName(_), stateV)  
//}
//
//class SyncTableColumnsValues[ColumnValueT](nrV: Int, jobnameV: String, timeV: Long, lockV: Boolean, onlineV: Boolean, completedV: Boolean, tablenameV: String, batchesV: Int, onlineVersionsV: Int, stateV: String) extends SyncTableColumns[ColumnV[ColumnValueT]] {
//  override val jobname = new ColumnV[String]("jobname", CassandraTypeName.getCassandraTypeName, jobnameV)
//  override val time = new ColumnV[Long]("time", CassandraTypeName.getCassandraTypeName, timeV)
//  override val lock = new ColumnV[Boolean]("lock", CassandraTypeName.getCassandraTypeName, lockV)
//  override val online = new ColumnV[Boolean]("online", CassandraTypeName.getCassandraTypeName, onlineV)
//  override val nr = new ColumnV[Int]("nr", CassandraTypeName.getCassandraTypeName, nrV)
//  override val batches = new ColumnV[Int]("batches", CassandraTypeName.getCassandraTypeName, batchesV)
//  override val onlineVersions = new ColumnV[Int]("onlineVersions", CassandraTypeName.getCassandraTypeName, onlineVersionsV)
//  override val tablename = new ColumnV[String]("tablename", CassandraTypeName.getCassandraTypeName, tablenameV)
//  override val completed = new ColumnV[Boolean]("completed", CassandraTypeName.getCassandraTypeName, completedV)
//  override val state =  new ColumnV[String]("state", CassandraTypeName.getCassandraTypeName(_), stateV)  
//}

//class SyncTableEmpty(keySpace: String) extends Table[SyncTableColumns[Column[_]]](keySpace: String, tableName = "\"SyncTable\"", columns = new SyncTableColumnsE()) {}                                                                                                                              
//class SyncTableWithValues(keySpace: String, nrV: Int, jobnameV: String, timeV: Long, lockV: Boolean, onlineV: Boolean, completedV: Boolean, tablenameV: String, batchesV: Int, onlineVersionsV: Int, stateV: String) extends Table[SyncTableColumnsValues[_]](keySpace: String, tableName = "\"SyncTable\"", columns = new SyncTableColumnsValues(nrV, jobnameV, timeV, lockV, onlineV, completedV, tablenameV, batchesV, onlineVersionsV, stateV)){}
//
/**
 * Data tables
 */

//class EmptyExampleDataColumns extends Columns[Column] {
//  override val time = new TypedColumn[Long]("time", CassandraTypeName.getCassandraTypeName)
//  val lock = new TypedColumn[Boolean]("lock", CassandraTypeName.getCassandraTypeName)
//  val completed = new TypedColumn[Boolean]("completed", CassandraTypeName.getCassandraTypeName)
//  val sum = new TypedColumn[Long]("sum", CassandraTypeName.getCassandraTypeName)
//  override val allVals: List[Column] = time :: lock :: sum :: completed :: Nil
//}
//
//class DataTable[T <: DataColumns](keySpace: String, tableName: String, columns: T) extends Table[DataColumns](keySpace, tableName, columns) {}
//
//abstract class DataColumns(timeV: Long) extends Columns[ColumnWithValue[]] {
//  override val time = new ColumnV[Long]("time", CassandraTypeName.getCassandraTypeName, timeV)
//  override val allVals: List[ColumnV[_]] = time :: Nil
//  
//  override def toString(): String = {
//    val columnNames = allVals.foldLeft("")((acc, column) => "|" + acc + column.name + "|")
//    val values = allVals.foldLeft("")((acc, column) => "|" + acc + column.value + "|")
//    
//    columnNames + "\n" + values
//  }
//}

//class SumDataColumns(timeV: Long, sumV: Long) extends DataColumns(timeV) {
//  override val time = new ColumnV[Long]("time", CassandraTypeName.getCassandraTypeName, timeV)
//  val sum = new ColumnV[Long]("sum", CassandraTypeName.getCassandraTypeName, sumV)
//  override val allVals: List[ColumnV[_]] = time :: sum :: Nil
//}
//
//object SumDataColumns {
//  def apply(timeV: Long, sumV: Long) = new SumDataColumns(timeV, sumV);
//}
//
//
//class CassandraSumDataTable(keySpace: String, tableName: String, columns: DataColumns) extends DataTable[DataColumns](keySpace, tableName, columns) {
//  QueryBuilder.insertInto(keySpace, tableName)
// 
//}