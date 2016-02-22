package scray.querying.sync.types

import scala.reflect.runtime.universe._
import shapeless._
import com.datastax.driver.core.Statement


case class Table[T <: Columns](val keySpace: String, val tableName: String, val columns: T) {}

trait Column[T] {
  val name: String
  protected val dbTypeDetector: Column[T] => String
  def getDBType = { dbTypeDetector(this) }
}

sealed trait Columns { 
  val time: Column[Long]

  def foldLeft[B](z: B)(f: (B, Column[_]) => B): B = {
    allVals.foldLeft(z)(f)
  }

  val allVals: List[Column[_]]
  lazy val primKey = "(" + time.name + ")"
  lazy val indexes: Option[List[String]] = None
}

abstract class DbSession[In1, T](val dbHostname: String) {
  def execute(statement: In1): T
  def execute(statement: String): T
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
      case _                                  => "text"
    }
  }
}


class SyncTableColumns extends Columns {
  val jobname = new ColumnE[String]("jobname", CassandraTypeName.getCassandraTypeName(_))
  override val time = new ColumnE[Long]("time", CassandraTypeName.getCassandraTypeName(_))
  val lock = new ColumnE[Boolean]("lock", CassandraTypeName.getCassandraTypeName(_))
  val online = new ColumnE[Boolean]("online", CassandraTypeName.getCassandraTypeName(_))
  val tablename = new ColumnE[String]("tablename", CassandraTypeName.getCassandraTypeName(_))
  val completed = new ColumnE[Boolean]("completed", CassandraTypeName.getCassandraTypeName(_))
  override lazy val primKey = "(" + jobname.name + ")"
  override lazy val indexes = Option(List(lock.name))
  override val allVals: List[Column[_]] = tablename :: jobname :: time :: lock :: online :: completed :: Nil
}

class SyncTableColumnsValues(jobnameV: String, timeV: Long, lockV: Boolean, onlineV: Boolean, completedV: Boolean, tablenameV: String) extends Columns {
  val jobname = new ColumnV[String]("jobname", CassandraTypeName.getCassandraTypeName, jobnameV)
  override val time = new ColumnV[Long]("time", CassandraTypeName.getCassandraTypeName, timeV)
  val lock = new ColumnV[Boolean]("lock", CassandraTypeName.getCassandraTypeName, lockV)
  val online = new ColumnV[Boolean]("online", CassandraTypeName.getCassandraTypeName, onlineV)
  val tablename = new ColumnV[String]("tablename", CassandraTypeName.getCassandraTypeName, tablenameV)
  val completed = new ColumnV[Boolean]("completed", CassandraTypeName.getCassandraTypeName, completedV)
  override lazy val primKey = "(" + jobname.name +")"
  override lazy val indexes = Option(List(lock.name))
  override val allVals: List[Column[_]] = tablename :: jobname :: time :: lock :: online :: completed :: Nil
}

class SyncTableEmpty(keySpace: String) extends Table(keySpace: String, tableName = "\"SyncTable\"", columns = new SyncTableColumns) {}
class SyncTableWithValues(keySpace: String, jobnameV: String, timeV: Long, lockV: Boolean, onlineV: Boolean, completedV: Boolean, tablenameV: String) extends Table(keySpace: String, tableName = "\"SyncTable\"", columns = new SyncTableColumnsValues(jobnameV, timeV, lockV, onlineV, completedV, tablenameV)){}

/**
 * Data tables
 */

class EmptyExampleDataColumns extends Columns {
  override val time = new ColumnE[Long]("time", CassandraTypeName.getCassandraTypeName)
  val lock = new ColumnE[Boolean]("lock", CassandraTypeName.getCassandraTypeName)
  val completed = new ColumnE[Boolean]("completed", CassandraTypeName.getCassandraTypeName)
  val sum = new ColumnE[Boolean]("sum", CassandraTypeName.getCassandraTypeName)
  override val allVals: List[Column[_]] = time :: lock :: completed :: Nil
}

class DataColumns(timeV: Long) extends Columns {
  override val time = new ColumnV[Long]("time", CassandraTypeName.getCassandraTypeName, timeV)
  override val allVals: List[Column[_]] = time :: Nil
}

class DataTable[T <: DataColumns](keySpace: String, tableName: String, columns: T) extends Table(keySpace, tableName, columns) {}
case class CassandraTableLocation(keySpace: String, table: String)


