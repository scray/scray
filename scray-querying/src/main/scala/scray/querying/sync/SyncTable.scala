package scray.querying.sync.types

import scala.reflect.runtime.universe._
import shapeless._
import com.datastax.driver.core.Statement
import scala.collection.JavaConverters._
import com.datastax.driver.core.querybuilder.Insert
import com.datastax.driver.core.querybuilder.QueryBuilder

abstract case class Table[T <: AbstractRows[_ <: Column[_]]](val keySpace: String, val tableName: String, val columns: T) {}

class Column[T] (
    val name: String,
    protected val dbTypeDetector: Column[T] => String
  ) {
    def getDBType = { dbTypeDetector(this) }
}

abstract class AbstractRows[ColumnT <: Column[_]] { 
  
  def foldLeft[B](z: B)(f: (B, ColumnT) => B): B = {
    columns.foldLeft(z)(f)
  }

  val columns: List[ColumnT] = Nil
  val primeryKey = ""
  val indexes: Option[List[Column[_]]] = None
}

class ColumnWithValue[ColumnT, ValueT](name: String, dbTypeDetector: Column[ColumnT] => String, val value: ValueT) extends Column[ColumnT](name, dbTypeDetector) {}
class RowWithValue[ColumnT <: ColumnWithValue[_, _]](columnsV: List[ColumnT], primaryKeyV: String, indexesV: Option[List[Column[_]]]) extends AbstractRows[ColumnWithValue[_, _]] {
  override val columns = columnsV
  override val primeryKey = primaryKeyV
  override val indexes = indexesV
  
  class ff extends Iterator[String] {
    def hasNext: Boolean = ???
    def next(): String = ???
  }
  
}

abstract class DbSession[Statement,InsertIn, Result](val dbHostname: String) {
  def execute(statement: Statement): Result
  def execute(statement: String): Result
  def insert(statement: InsertIn): Result
}


///**
// * Column with values.
// */
//class ColumnV[T](nname: String, ndbTypeDetector: Column => String, val value: T) extends Column { 
//  override val name = nname
//  override protected val dbTypeDetector = ndbTypeDetector
//}
//
//
//object CassandraTypeName {
//  def getCassandraTypeName(value: Column): String = {
//    value match {
//      case _ if typeOf[value.T] =:= typeOf[String]  => "text"
//      case _ if typeOf[value.T] =:= typeOf[Boolean] => "boolean"
//      case _ if typeOf[value.T] =:= typeOf[Long]    => "bigint"
//      case _ if typeOf[value.T] =:= typeOf[Int]     => "int"
//      case _                                        => "text"
//    }
//  }
//}
//object SyncTableInitFunction {
//  type TableInitFunctionT[ColumnClass <: Column, ParamsT, ColumnType] = Tuple2[ParamsT => ColumnClass, ParamsT]
//  
//  implicit def createColumnV[T](name: String, dbTypeDetector: Column => String, value: T): ColumnV[T] = new ColumnV[T](name, dbTypeDetector, value)
//  
//  def createEmptyColumn[ColumnType](nname: String): Column = {
//    new Column {
//      override val name = nname
//      override protected val dbTypeDetector = CassandraTypeName.getCassandraTypeName(_: Column)
//    }
//  }
//
//}
//
//
//import scray.querying.sync.types.SyncTableInitFunction.TableInitFunctionT
//import scray.querying.sync.types.SyncTableInitFunction.{createEmptyColumn, createColumnV}
//class SyncTableColumns[ColumnT <: Column , Params](
//    jobnameInit: TableInitFunctionT[ColumnT, Params, String],
//    versionNrInit: TableInitFunctionT[ColumnT, Params, Int],
//    stateInit: TableInitFunctionT[ColumnT, Params, String],
//    batchVersionsInit: TableInitFunctionT[ColumnT, Params, Int],
//    onlineVersionsInit: TableInitFunctionT[ColumnT, Params, Int],
//    lockedInit: TableInitFunctionT[ColumnT, Params, Boolean],
//    onlineInit: TableInitFunctionT[ColumnT, Params, Boolean],
//    completedInit: TableInitFunctionT[ColumnT, Params, Boolean],
//    tablenameInit: TableInitFunctionT[ColumnT, Params, String]
//) extends Columns[Column] {
//  
//  val jobname = jobnameInit._1 (jobnameInit._2)
//  val versionNr = versionNrInit._1 (versionNrInit._2)
//  val batchesVersions = batchVersionsInit._1 (batchVersionsInit._2)
//  val onlineVersions = onlineInit._1 (onlineInit._2)
//  val locked = lockedInit._1 (lockedInit._2)
//  val online = onlineInit._1 (onlineInit._2)
//  val tablename = tablenameInit._1 (tablenameInit._2)
//  val completed = completedInit._1 (completedInit._2)
//  val state = stateInit._1 (stateInit._2)
//  override lazy val allVals: List[Column] = time :: jobname :: locked :: online :: versionNr :: batchesVersions :: onlineVersions :: tablename :: completed :: state :: Nil
//  override lazy val primKey = s"(${jobname.name}, ${online.name}, ${versionNr.name})"
//  override lazy val indexes = Option(List(locked.name))
//}
//
//object SyncTableColumns {
//  def apply() = {
//    new SyncTableColumns(
//        (createEmptyColumn[String]  _, "jobname"),
//        (createEmptyColumn[Int]     _, "versionNr"),
//        (createEmptyColumn[String]  _, "state"),
//        (createEmptyColumn[Int]     _, "batchVersions"),
//        (createEmptyColumn[Int]     _, "onlineVersions"),
//        (createEmptyColumn[Boolean] _, "locked"),
//        (createEmptyColumn[Boolean] _, "online"),
//        (createEmptyColumn[Boolean] _, "completed"),
//        (createEmptyColumn[String]  _, "tablename")
//        )
//  }
//}
//
//class DataColumns[ColumnT <: ColumnWithValue[_] , Params, F](
//    jobnameInit: TableInitFunctionT[ColumnT, Params, String],
//    versionNrInit: TableInitFunctionT[ColumnT, Params, Int],
//    stateInit: TableInitFunctionT[ColumnT, Params, String],
//    batchVersionsInit: TableInitFunctionT[ColumnT, Params, Int],
//    onlineVersionsInit: TableInitFunctionT[ColumnT, Params, Int],
//    lockedInit: TableInitFunctionT[ColumnT, Params, Boolean],
//    onlineInit: TableInitFunctionT[ColumnT, Params, Boolean],
//    completedInit: TableInitFunctionT[ColumnT, Params, Boolean],
//    tablenameInit: TableInitFunctionT[ColumnT, Params, String]
//) extends Columns[Column] {
//  
//  val jobname = jobnameInit._1 (jobnameInit._2)
//  val versionNr = versionNrInit._1 (versionNrInit._2)
//  val batchesVersions = batchVersionsInit._1 (batchVersionsInit._2)
//  val onlineVersions = onlineInit._1 (onlineInit._2)
//  val locked = lockedInit._1 (lockedInit._2)
//  val online = onlineInit._1 (onlineInit._2)
//  val tablename = tablenameInit._1 (tablenameInit._2)
//  val completed = completedInit._1 (completedInit._2)
//  val state = stateInit._1 (stateInit._2)
//  override lazy val allVals: List[Column] = time :: jobname :: locked :: online :: versionNr :: batchesVersions :: onlineVersions :: tablename :: completed :: state :: Nil
//  override lazy val primKey = s"(${jobname.name}, ${online.name}, ${versionNr.name})"
//  override lazy val indexes = Option(List(locked.name))
//}
//
//object DataColumns {
//  def apply() = {
//    new SyncTableColumns(
//        (createEmptyColumn[String]  _, "jobname"),
//        (createEmptyColumn[Int]     _, "versionNr"),
//        (createEmptyColumn[String]  _, "state"),
//        (createEmptyColumn[Int]     _, "batchVersions"),
//        (createEmptyColumn[Int]     _, "onlineVersions"),
//        (createEmptyColumn[Boolean] _, "locked"),
//        (createEmptyColumn[Boolean] _, "online"),
//        (createEmptyColumn[Boolean] _, "completed"),
//        (createEmptyColumn[String]  _, "tablename")
//        )
//  }
//}
//
//
//
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