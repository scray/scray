package scray.querying.sync.types

import scala.reflect.runtime.universe._
import shapeless._
import com.datastax.driver.core.Statement
import scala.collection.JavaConverters._
import com.datastax.driver.core.querybuilder.Insert
import com.datastax.driver.core.querybuilder.QueryBuilder


abstract case class Table[T <: Columns[_ <: Column[_]]](val keySpace: String, val tableName: String, val columns: T) {}

class Column[T](
    val name: String, 
    protected val dbTypeDetector: Column[T] => String) {
  
  def getDBType = { dbTypeDetector(this) }
}

abstract case class Columns[ColumnT <: Column[_]]() { 
  val time: Column[Long]

  def create[ColumnT](implicit manifest : Manifest[ColumnT]) = manifest.erasure.newInstance.asInstanceOf[ColumnT]
  
  def foldLeft[B](z: B)(f: (B, ColumnT) => B): B = {
    allVals.foldLeft(z)(f)
  }

  val allVals: List[ColumnT]
  lazy val primKey = "(" + time.name + ")"
  lazy val indexes: Option[List[String]] = None
}

abstract class DbSession[Statement,InsertIn, Result](val dbHostname: String) {
  def execute(statement: Statement): Result
  def execute(statement: String): Result
  def insert(statement: InsertIn): Result
}


/**
 * Column with values.
 */
class ColumnV[T](name: String, val dbTypeDetector: Column[T] => String, val value: T) extends Column[T](name, dbTypeDetector)

object CassandraTypeName {
  def getCassandraTypeName[T: TypeTag](value: Column[T]): String = {
    value match {
      case _ if typeOf[T] =:= typeOf[String]  => "text"
      case _ if typeOf[T] =:= typeOf[Boolean] => "boolean"
      case _ if typeOf[T] =:= typeOf[Long]    => "bigint"
      case _ if typeOf[T] =:= typeOf[Int]    =>  "int"
      case _                                  => "text"
    }
  }
}


abstract class SyncTableColumns[ColumnType <: Column[_]]() extends Columns[ColumnType] {
  val jobname: ColumnType 
  val nr : ColumnType 
  val batches : ColumnType 
  val onlineVersions : ColumnType 
  val lock : ColumnType 
  val online : ColumnType 
  val tablename : ColumnType 
  val completed : ColumnType 
  val state: ColumnType 
  override val allVals = jobname :: lock :: online :: nr :: batches :: onlineVersions :: tablename :: completed :: state :: Nil
  override lazy val primKey = s"(${jobname.name}, ${online.name}, ${nr.name})"
  override lazy val indexes = Option(List(lock.name))
}

class SyncTableColumnsE() extends SyncTableColumns[Column[_]] {
  override val jobname = new Column[String]("jobname", CassandraTypeName.getCassandraTypeName(_))
  override val time = new Column[Long]("time", CassandraTypeName.getCassandraTypeName(_))
  override val lock = new Column[Boolean]("lock", CassandraTypeName.getCassandraTypeName(_))
  override val online = new Column[Boolean]("online", CassandraTypeName.getCassandraTypeName(_))
  override val nr = new Column[Int]("nr", CassandraTypeName.getCassandraTypeName(_))
  override val batches = new Column[Int]("batches", CassandraTypeName.getCassandraTypeName(_))
  override val onlineVersions = new Column[Int]("onlineVersions", CassandraTypeName.getCassandraTypeName(_))
  override val tablename = new Column[String]("tablename", CassandraTypeName.getCassandraTypeName(_))
  override val completed = new Column[Boolean]("completed", CassandraTypeName.getCassandraTypeName(_))
  override val state =  new Column[String]("state", CassandraTypeName.getCassandraTypeName(_))
}

class SyncTableColumnsValues(nrV: Int, jobnameV: String, timeV: Long, lockV: Boolean, onlineV: Boolean, completedV: Boolean, tablenameV: String, batchesV: Int, onlineVersionsV: Int, stateV: String) extends SyncTableColumns[ColumnV[_]] {
  override val jobname = new ColumnV[String]("jobname", CassandraTypeName.getCassandraTypeName, jobnameV)
  override val time = new ColumnV[Long]("time", CassandraTypeName.getCassandraTypeName, timeV)
  override val lock = new ColumnV[Boolean]("lock", CassandraTypeName.getCassandraTypeName, lockV)
  override val online = new ColumnV[Boolean]("online", CassandraTypeName.getCassandraTypeName, onlineV)
  override val nr = new ColumnV[Int]("nr", CassandraTypeName.getCassandraTypeName, nrV)
  override val batches = new ColumnV[Int]("batches", CassandraTypeName.getCassandraTypeName, batchesV)
  override val onlineVersions = new ColumnV[Int]("onlineVersions", CassandraTypeName.getCassandraTypeName, onlineVersionsV)
  override val tablename = new ColumnV[String]("tablename", CassandraTypeName.getCassandraTypeName, tablenameV)
  override val completed = new ColumnV[Boolean]("completed", CassandraTypeName.getCassandraTypeName, completedV)
  override val state =  new ColumnV[String]("state", CassandraTypeName.getCassandraTypeName(_), stateV)
  
}

class SyncTableEmpty(keySpace: String) extends Table[SyncTableColumns[Column[_]]](keySpace: String, tableName = "\"SyncTable\"", columns = new SyncTableColumnsE()) {}                                                                                                                              
class SyncTableWithValues(keySpace: String, nrV: Int, jobnameV: String, timeV: Long, lockV: Boolean, onlineV: Boolean, completedV: Boolean, tablenameV: String, batchesV: Int, onlineVersionsV: Int, stateV: String) extends Table[SyncTableColumnsValues](keySpace: String, tableName = "\"SyncTable\"", columns = new SyncTableColumnsValues(nrV, jobnameV, timeV, lockV, onlineV, completedV, tablenameV, batchesV, onlineVersionsV, stateV)){}

/**
 * Data tables
 */

class EmptyExampleDataColumns extends Columns[Column[_]] {
  override val time = new Column[Long]("time", CassandraTypeName.getCassandraTypeName)
  val lock = new Column[Boolean]("lock", CassandraTypeName.getCassandraTypeName)
  val completed = new Column[Boolean]("completed", CassandraTypeName.getCassandraTypeName)
  val sum = new Column[Long]("sum", CassandraTypeName.getCassandraTypeName)
  override val allVals: List[Column[_]] = time :: lock :: sum :: completed :: Nil
}

class DataTable[T <: DataColumns](keySpace: String, tableName: String, columns: T) extends Table[DataColumns](keySpace, tableName, columns) {}

abstract class DataColumns(timeV: Long) extends Columns[ColumnV[_]] {
  override val time = new ColumnV[Long]("time", CassandraTypeName.getCassandraTypeName, timeV)
  override val allVals: List[ColumnV[_]] = time :: Nil
  
  override def toString(): String = {
    val columnNames = allVals.foldLeft("")((acc, column) => "|" + acc + column.name + "|")
    val values = allVals.foldLeft("")((acc, column) => "|" + acc + column.value + "|")
    
    columnNames + "\n" + values
  }
}

class SumDataColumns(timeV: Long, sumV: Long) extends DataColumns(timeV) {
  override val time = new ColumnV[Long]("time", CassandraTypeName.getCassandraTypeName, timeV)
  val sum = new ColumnV[Long]("sum", CassandraTypeName.getCassandraTypeName, sumV)
  override val allVals: List[ColumnV[_]] = time :: sum :: Nil
}

object SumDataColumns {
  def apply(timeV: Long, sumV: Long) = new SumDataColumns(timeV, sumV);
}


class CassandraSumDataTable(keySpace: String, tableName: String, columns: DataColumns) extends DataTable[DataColumns](keySpace, tableName, columns) {
  QueryBuilder.insertInto(keySpace, tableName)
 
}