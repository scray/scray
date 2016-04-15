package scray.querying.sync.types

import com.websudos.phantom.CassandraPrimitive._
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.universe._
import scala.util.Try
import java.util.concurrent.locks.Lock
import scray.querying.sync.cassandra.CassandraImplementation._
import scray.querying.sync.JobInfo




class Table[T <: AbstractRow](val keySpace: String, val tableName: String, val columns: T) extends Serializable {
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

class Column[T: DBColumnImplementation](val name: String) extends Serializable { self =>
  val dbimpl = implicitly[DBColumnImplementation[T]]
 
  // type DB_TYPE
  def getDBType: String = dbimpl.getDBType
}

abstract class AbstractRow extends Serializable {
  type ColumnType <: Column[_]
  val columns: List[ColumnType]
  val primaryKey = ""
  val indexes: Option[List[String]] = None

  def foldLeft[B](z: B)(f: (B, ColumnType) => B): B = {
    columns.foldLeft(z)(f)
  }
}

abstract class ArbitrarylyTypedRows extends AbstractRow {
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
    override val indexes: Option[List[String]]) extends AbstractRow {

  override type ColumnType = ColumnWithValue[_]

  def copy(): RowWithValue = {
    val columnCopies = this.columns.foldLeft(List[ColumnWithValue[_]]())((acc, column) => { column.clone() :: acc })
    new RowWithValue(columnCopies, this.primaryKey, this.indexes)
  }
}

class Columns(
  override val columns: List[Column[_]],
  override val primaryKey: String,
  override val indexes: Option[List[String]]) extends AbstractRow {
  
  override type ColumnType = Column[_]
}

abstract class DbSession[Statement, InsertIn, Result](val dbHostname: String) {
  def execute(statement: Statement): Try[Result]
  def execute(statement: String): Try[Result]
  def insert(statement: InsertIn): Try[Result]
}

object SyncTable {
  def apply(keySpace: String, tableName: String) = {
    new Table(keySpace, tableName, new SyncTableBasicClasses.SyncTableRowEmpty)
  }
}

object JobLockTable {
  def apply(keySpace: String, tableName: String) = {
    new Table(keySpace, tableName, new SyncTableBasicClasses.JobLockTable)
  }
}

object VoidTable {
  def apply(keySpace: String, tableName: String, columns: AbstractRow) = {
    new Table(keySpace, tableName, columns)
  }
}
object DataTable {
  def apply(keySpace: String, tableName: String, columns: RowWithValue) = {
    new Table(keySpace, tableName, columns)
  }
}

object SyncTableBasicClasses extends Serializable {

  class SyncTableRowEmpty() extends ArbitrarylyTypedRows {

    val jobname = new Column[String]("jobname")
    val slot = new Column[Int]("slot")
    val latestUpdate = new Column[Long]("latestUpdate")
    val versions = new Column[Int]("versions")
    val tableidentifier = new Column[String]("tableidentifier")
    val batchStartTime = new Column[Long]("batchStartTime")
    val batchEndTime = new Column[Long]("batchEndTime")
    val locked = new Column[Boolean]("locked")
    val online = new Column[Boolean]("online")
    val state = new Column[String]("state")

    override val columns = jobname :: latestUpdate :: slot :: versions :: tableidentifier :: locked :: online :: state :: batchStartTime :: batchEndTime ::Nil
    override val primaryKey = s"((${jobname.name}, ${online.name}), ${slot.name})"
    override val indexes: Option[List[String]] = Option(List(locked.name, state.name, batchEndTime.name, batchStartTime.name))
  }
  
  class JobLockTable() extends ArbitrarylyTypedRows {

    val jobname = new Column[String]("jobname")
    val locked = new Column[Boolean]("locked")

    override val columns = jobname :: locked ::Nil
    override val primaryKey = s"(${jobname.name})"
    override val indexes: Option[List[String]] = None
  }
}

object State extends Enumeration {
  type State = Value
  val NEW, RUNNING, COMPLETED, OBSOLETE, TRANSFER = Value
}

abstract class LockApi[Statement, Insert, Result](
      val job: JobInfo[Statement, Insert, Result], 
      val jobLockTable: Table[SyncTableBasicClasses.JobLockTable], 
      val dbSession: DbSession[Statement, Insert, Result]) extends Lock {
  
  def this(job: JobInfo[Statement, Insert, Result], dbSession: DbSession[Statement, Insert, Result]) {
    this(job, JobLockTable("SILIDX", "jobLock"), dbSession)
  }
  
}