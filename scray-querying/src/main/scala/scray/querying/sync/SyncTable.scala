package scray.querying.sync.types

import scala.reflect.runtime.universe._
import shapeless._
import com.datastax.driver.core.Statement
import scala.collection.JavaConverters._
import com.datastax.driver.core.querybuilder.Insert
import com.datastax.driver.core.querybuilder.QueryBuilder


abstract case class Table[T <: Columns[_ <: Column[_]]](val keySpace: String, val tableName: String, val columns: T) {}

trait Column[T] {
  val name: String
  protected val dbTypeDetector: Column[T] => String
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
 * Column without values.
 */
case class ColumnE[T](name: String, dbTypeDetector: Column[T] => String) extends Column[T]

/**
 * Column with values.
 */
case class ColumnV[T](name: String, dbTypeDetector: Column[T] => String, value: T) extends Column[T]

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


class SyncTableColumns() extends Columns[Column[_]] {
  val jobname = new ColumnE[String]("jobname", CassandraTypeName.getCassandraTypeName(_))
  override val time = new ColumnE[Long]("time", CassandraTypeName.getCassandraTypeName(_))
  val lock = new ColumnE[Boolean]("lock", CassandraTypeName.getCassandraTypeName(_))
  val online = new ColumnE[Boolean]("online", CassandraTypeName.getCassandraTypeName(_))
  val nr = new ColumnE[Int]("nr", CassandraTypeName.getCassandraTypeName(_))
  val batches = new ColumnE[Int]("batches", CassandraTypeName.getCassandraTypeName(_))
  val onlineVersions = new ColumnE[Int]("onlineVersions", CassandraTypeName.getCassandraTypeName(_))
  val tablename = new ColumnE[String]("tablename", CassandraTypeName.getCassandraTypeName(_))
  val completed = new ColumnE[Boolean]("completed", CassandraTypeName.getCassandraTypeName(_))
  override lazy val primKey = s"(${jobname.name}, ${online.name}, ${nr.name})"
  override lazy val indexes = Option(List(lock.name))
  override val allVals = tablename :: jobname ::batches :: onlineVersions :: time :: nr :: lock :: online :: completed :: Nil
}

class SyncTableColumnsValues(nrV: Int, jobnameV: String, timeV: Long, lockV: Boolean, onlineV: Boolean, completedV: Boolean, tablenameV: String, batchesV: Int, onlineVersionsV: Int) extends Columns[ColumnV[_]] {
  val jobname = new ColumnV[String]("jobname", CassandraTypeName.getCassandraTypeName, jobnameV)
  override val time = new ColumnV[Long]("time", CassandraTypeName.getCassandraTypeName, timeV)
  val lock = new ColumnV[Boolean]("lock", CassandraTypeName.getCassandraTypeName, lockV)
  val online = new ColumnV[Boolean]("online", CassandraTypeName.getCassandraTypeName, onlineV)
  val nr = new ColumnV[Int]("nr", CassandraTypeName.getCassandraTypeName, nrV)
  val batches = new ColumnV[Int]("batches", CassandraTypeName.getCassandraTypeName, batchesV)
  val onlineVersions = new ColumnV[Int]("onlineVersions", CassandraTypeName.getCassandraTypeName, onlineVersionsV)
  val tablename = new ColumnV[String]("tablename", CassandraTypeName.getCassandraTypeName, tablenameV)
  val completed = new ColumnV[Boolean]("completed", CassandraTypeName.getCassandraTypeName, completedV)
  override lazy val primKey = s"(${jobname.name}, ${online.name}, ${nr.name})"
  override lazy val indexes = Option(List(lock.name))
  override val allVals = tablename :: jobname :: time :: batches :: onlineVersions :: nr :: lock :: online :: completed :: Nil
}

class SyncTableEmpty(keySpace: String) extends Table[SyncTableColumns](keySpace: String, tableName = "\"SyncTable\"", columns = new SyncTableColumns) {}                                                                                                                              
class SyncTableWithValues(keySpace: String, nrV: Int, jobnameV: String, timeV: Long, lockV: Boolean, onlineV: Boolean, completedV: Boolean, tablenameV: String, batchesV: Int, onlineVersionsV: Int) extends Table[SyncTableColumnsValues](keySpace: String, tableName = "\"SyncTable\"", columns = new SyncTableColumnsValues(nrV, jobnameV, timeV, lockV, onlineV, completedV, tablenameV, batchesV, onlineVersionsV)){}

/**
 * Data tables
 */

class EmptyExampleDataColumns extends Columns[ColumnE[_]] {
  override val time = new ColumnE[Long]("time", CassandraTypeName.getCassandraTypeName)
  val lock = new ColumnE[Boolean]("lock", CassandraTypeName.getCassandraTypeName)
  val completed = new ColumnE[Boolean]("completed", CassandraTypeName.getCassandraTypeName)
  val sum = new ColumnE[Long]("sum", CassandraTypeName.getCassandraTypeName)
  override val allVals: List[ColumnE[_]] = time :: lock :: sum :: completed :: Nil
}

class DataTable[T <: DataColumns](keySpace: String, tableName: String, columns: T) extends Table[DataColumns](keySpace, tableName, columns) {}

case class CassandraTableLocation(keySpace: String, table: String)

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