package scray.querying.sync

import java.util.{ Iterator => JIterator }

import scala.annotation.tailrec

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.RegularStatement
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Row
import com.datastax.driver.core.Session
import com.datastax.driver.core.SimpleStatement
import com.datastax.driver.core.Statement
import com.datastax.driver.core.querybuilder.Insert
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.typesafe.scalalogging.slf4j.LazyLogging
import scray.querying.sync.types._

import scray.querying.sync.types._


abstract class OnlineBatchSync[T <: DataColumns] extends LazyLogging {

  /**
   * Generate and register tables for a new job.
   */
  def initJob(jobName: String, numberOfBatches: Int, dataTable: T)

  /**
   * Lock online table if it is used by another spark job.
   */
  def lockOnlineTable(jobName: String, nr: Int): Boolean
   /**
   * Unlock online table to make it available for a new job.
   */
  def unlockOnlineTable(jobName: String, nr: Int): Boolean
  def isOnlineTableLocked(jobName: String, nr: Int): Boolean
  
   /**
   * Lock online table if it is used by another spark job.
   */
  def lockBatchTable(jobName: String, nr: Int): Boolean
  def isBatchTableLocked(jobName: String, nr: Int): Boolean

   /**
   * Unlock batch table to make it available for a new job.
   */
  def unlockBatchTable(jobName: String, nr: Int): Boolean
  
  def getHeadBatch(jobName: String): Option[CassandraTableLocation]
  
  def insertInBatchTable(jobName: String, nr: Int, data: DataColumns)
  def insertInOnlineTable(jobName: String, nr: Int, data: DataColumns)
  
  //def getJobData[ColumnsT <: Columns[_]](jobName: String, nr: Int): ColumnsT

  
}

class OnlineBatchSyncCassandra[T <: DataColumns](dbHostname: String, dbSession: Option[DbSession[Statement, Insert, ResultSet]]) extends OnlineBatchSync[T]  {

  // Create or use a given DB session.
  val session = dbSession.getOrElse(new DbSession[SimpleStatement, Insert, ResultSet](dbHostname) {
    val cassandraSession = Cluster.builder().addContactPoint(dbHostname).build().connect()

    override def execute(statement: String): ResultSet = {
      cassandraSession.execute(statement)
    }

    def execute(statement: Statement): ResultSet = {
      cassandraSession.execute(statement)
    }
    
    def insert(statement: Insert): ResultSet = {
      cassandraSession.execute(statement)
    }

    def execute(statement: SimpleStatement): ResultSet = {
      cassandraSession.execute(statement)
    }
  })

  val syncTable: Table[SyncTableColumns] = new SyncTableEmpty("\"ABC\"")

  /**
   * Generate and register tables for a new job.
   */
  def initJob(jobName: String, numberOfBatches: Int, dataColumns: T): Unit = {
    createKeyspace[SyncTableColumns](syncTable)
    syncTable.columns.indexes match {
      case _: Some[List[String]] => session.execute(createIndexString(syncTable))
      case _                     =>
    }

    // Create data tables and register them in sync table
    1 to numberOfBatches foreach { i =>

      // Create batch data tables
      session.execute(createSingleTableString(new DataTable(syncTable.keySpace, getBatchJobName(jobName, i), dataColumns)))

      // Register batch table
      session.execute(QueryBuilder.insertInto(syncTable.keySpace, syncTable.tableName)
        .value(syncTable.columns.jobname.name, jobName)
        .value(syncTable.columns.online.name, false)
        .value(syncTable.columns.nr.name, i)
        .value(syncTable.columns.tablename.name, getBatchJobName(syncTable.keySpace + "." + jobName, i))
        .value(syncTable.columns.completed.name, false)
        .value(syncTable.columns.lock.name, false)
        .value(syncTable.columns.time.name, System.currentTimeMillis()).toString())
    }

    // Create and register online tables
    1 to 3 foreach { i =>
      // Create online data tables
      // Columns[Column[_]]
      val ff = new DataTable[DataColumns](syncTable.keySpace, getOnlineJobName(jobName, i), dataColumns)
      session.execute(createSingleTableString(new DataTable(syncTable.keySpace, getOnlineJobName(jobName, i), dataColumns)))

      // Register online tables
      session.execute(QueryBuilder.insertInto(syncTable.keySpace, syncTable.tableName)
        .value(syncTable.columns.online.name, true)
        .value(syncTable.columns.jobname.name, jobName)
        .value(syncTable.columns.nr.name, i)
        .value(syncTable.columns.tablename.name, getOnlineJobName(syncTable.keySpace + "." + jobName, i))
        .value(syncTable.columns.completed.name, false)
        .value(syncTable.columns.lock.name, false)
        .value(syncTable.columns.time.name, System.currentTimeMillis()).toString())
    }
  }

  def getHeadBatch(jobName: String): Option[CassandraTableLocation] = {
    val headBatchQuery: RegularStatement = QueryBuilder.select().from(syncTable.keySpace + "." + syncTable.tableName).
      where(QueryBuilder.eq(syncTable.columns.jobname.name, jobName))
    val headBatches = this.execute(headBatchQuery)

    // Find newest version 
    val newestBatch = this.getNewestRow(headBatches.iterator())
    Option(CassandraTableLocation(syncTable.keySpace, newestBatch.getLong(syncTable.columns.time.name).toString()))
  }
  
  def insertInOnlineTable(jobName: String, nr: Int, data: DataColumns) {
    if(this.lockOnlineTable(jobName, nr)) {
      val statement = data.foldLeft(QueryBuilder.insertInto(syncTable.keySpace, getOnlineJobName(jobName, nr))) {
        (acc, column) => acc.value(column.name, column.value)
      }
      session.insert(statement)
      this.unlockBatchTable(jobName, nr)
    } else {
      logger.error(s"Online table for job ${jobName} is locked. It is not possible to insert Data.")
    }
  }
  
  
  def insertInBatchTable(jobName: String, nr: Int, data: DataColumns) {
    if(this.lockBatchTable(jobName, nr)) { 
      val statement = data.foldLeft(QueryBuilder.insertInto(syncTable.keySpace, getBatchJobName(jobName, nr))) {
        (acc, column) => acc.value(column.name, column.value)
      }
      session.insert(statement)
      this.unlockBatchTable(jobName, nr)
    } else {
      logger.error(s"Online table for job ${jobName} is locked. It is not possible to insert Data.")
    }
  }

//  def getTailBatch(jobName: String, session: Session): Option[CassandraTableLocation] = {
//    val headBatchQuery: RegularStatement = QueryBuilder.select().from(table.keySpace + "." + table.columnFamily).
//      where().and((QueryBuilder.eq(table.columns(0)._1, jobName))).and((QueryBuilder.eq(table.columns(3)._1, false)))
//    val headBatches = session.execute(headBatchQuery)
//
//    // Find newest version 
//    val newestBatch = this.getOldestRow(headBatches.iterator())
//    Option(CassandraTableLocation(table.keySpace, newestBatch.getString(5)))
//  }

  /**
   * Create base keyspace and table
   */
  def createKeyspace[F <: Columns[_ <: Column[_]]](table: Table[F]): Unit = {
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS ${table.keySpace} WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1};")
    //   def createSingleTableString[T <: Columns[F], F <: Column[String]](table: Table[T]): String = {
    session.execute(createSingleTableString(table))
  }

  /**
   * Lock table if it is used by another spark job.
   */
  def lockOnlineTable(jobName: String, nr: Int): Boolean = {
    logger.debug(s"Lock online table for job: ${jobName} ")
    setLock(jobName, nr, true, true)
  }
  
   /**
   * Lock online table if it is used by another spark job.
   */
  def lockBatchTable(jobName: String, nr: Int): Boolean = {
    logger.debug(s"Lock batch table for job: ${jobName} ${nr}")
    setLock(jobName, nr, false, true)
  }

  /**
   * Unlock batch table to make it available for a new job.
   */
  def unlockBatchTable(jobName: String, nr: Int): Boolean = {
    logger.debug(s"Unlock batch table for job: ${jobName} ${nr}")
    setLock(jobName, nr, false, false)
  }
  
   /**
   * Unlock batch table to make it available for a new job.
   */
  def unlockOnlineTable(jobName: String, nr: Int): Boolean =  {
    logger.debug(s"Unlock online table for job: ${jobName} ${nr}")
    
    setLock(jobName, nr, true, false)
  }


  def isOnlineTableLocked(jobName: String, nr: Int): Boolean = {
    logger.debug(s"Unlock table for job: ${jobName}")
    val res = execute(QueryBuilder.select().all().from(syncTable.keySpace, syncTable.tableName).where(
      QueryBuilder.eq(syncTable.columns.jobname.name, jobName)).
       and(QueryBuilder.eq(syncTable.columns.online.name, true)).
       and(QueryBuilder.eq(syncTable.columns.nr.name, nr)).
       and(QueryBuilder.eq(syncTable.columns.lock.name, true)))
    (res.all().size() > 0)
  }
  
  def isBatchTableLocked(jobName: String, nr: Int): Boolean = {
    logger.debug(s"Unlock table for job: ${jobName}")
    val res = execute(QueryBuilder.select().all().from(syncTable.keySpace, syncTable.tableName).
        where(QueryBuilder.eq(syncTable.columns.jobname.name, jobName)).
       and(QueryBuilder.eq(syncTable.columns.online.name, false)).
       and(QueryBuilder.eq(syncTable.columns.nr.name, nr)).
       and(QueryBuilder.eq(syncTable.columns.lock.name, true)))
    (res.all().size() > 0)
  }

  private def setLock(jobName: String, nr: Int, online: Boolean, newState: Boolean): Boolean = {
    executeQuorum(QueryBuilder.update(syncTable.keySpace + "." + syncTable.tableName).
        `with`(QueryBuilder.set(syncTable.columns.lock.name, newState)).
        where(QueryBuilder.eq(syncTable.columns.jobname.name, jobName)).
        and(QueryBuilder.eq(syncTable.columns.online.name, online)).
        and(QueryBuilder.eq(syncTable.columns.nr.name, nr)).
        onlyIf(QueryBuilder.eq(syncTable.columns.lock.name, !newState)))   
  }
  def selectAll() = {
    val rows = execute(QueryBuilder.select().all().from(syncTable.keySpace, syncTable.tableName))
    
    println(rows.getColumnDefinitions)
    val iter = rows.all().iterator()
    while(iter.hasNext()) {
      println(iter.next())
    }
  }
  
  def getOnlineJobData(jobname: String, nr: Int): SumDataColumns = {   
      val rows = execute(QueryBuilder.select().all().from(syncTable.keySpace, getOnlineJobName(jobname, nr)))
      val iter = rows.iterator()
      val sumDataColumns = new SumDataColumns(1L, 2L)
      val columns = rows.all().get(0)
      
      SumDataColumns(columns.getLong(sumDataColumns.time.name), columns.getLong(sumDataColumns.sum.name))
  }
  
  private def executeQuorum(statement: RegularStatement): Boolean = {
    println(statement.toString())
    logger.debug("Lock table: " + statement)
    val simpleStatement = new SimpleStatement(statement.toString())
    simpleStatement.setConsistencyLevel(ConsistencyLevel.ALL)

    val rs = session.execute(simpleStatement);
    val row = rs.one();
     
    if (row.getBool("[applied]")) {
      logger.debug(s"Execute ${simpleStatement}");
      true
    } else {
      logger.error(s"It is currently not possible to execute ${simpleStatement} ")
      false
    }
  }

  private def execute(statement: RegularStatement) = {
    println(statement.toString())
    logger.debug("Lock table: " + statement)
    val simpleStatement = new SimpleStatement(statement.toString())
    simpleStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);

    session.execute(simpleStatement);
  }

  def createSingleTableString[F <: Columns[_ <: Column[_]]](table: Table[F]): String = {
    val createStatement = s"CREATE TABLE IF NOT EXISTS ${table.keySpace + "." + table.tableName} (" +
      s"${table.columns.foldLeft("")((acc, next) => { acc + next.name + " " + next.getDBType + ", " })} " +
      s"PRIMARY KEY ${table.columns.primKey})"
    logger.debug(s"Create table String: ${createStatement} ")
    println(s"Create table String: ${createStatement} ")
    createStatement
  }

  private def createIndexString[T <: Columns[Column[_]]](table: Table[T]): String = {
    s"CREATE INDEX IF NOT EXISTS ON ${table.keySpace}.${table.tableName} (${table.columns.indexes.getOrElse(List("")).head})"
  }

  private def getBatchJobName(jobname: String, nr: Int): String = { jobname + "_batch" + nr }
  private def getOnlineJobName(jobname: String, nr: Int): String = { jobname + "_online" + nr }
  
      def getNewestRow(rows: java.util.Iterator[Row]): Row = {
      import scala.math.Ordering._
      val comp = implicitly[Ordering[Long]]
      getComptRow(rows, comp.gt)
    }
    
    def getOldestRow(rows: java.util.Iterator[Row]): Row = {
      import scala.math.Ordering._
      val comp = implicitly[Ordering[Long]]
      getComptRow(rows, comp.lt)
    }
    
    def getComptRow(rows: JIterator[Row], comp: (Long, Long) => Boolean): Row = {
      @tailrec def accNewestRow(prevRow: Row, nextRows: JIterator[Row]): Row = {
        if(nextRows.hasNext) {
          val localRow = nextRows.next()
          println()
          val max = if(comp(prevRow.getLong(syncTable.columns.time.name), localRow.getLong(syncTable.columns.time.name))) {
            prevRow
          } else {
            localRow
          }
          accNewestRow(max, nextRows)
        } else {
          prevRow
        }
      }
      import scala.collection.convert.WrapAsScala.asScalaIterator
      
      val row = rows.next()
      accNewestRow(row, rows)
    }
}