package scray.querying.sync.cassandra

import com.websudos.phantom.CassandraPrimitive
import scray.querying.sync.types.DBColumnImplementation
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
import java.util.ArrayList
import scala.collection.mutable.ArrayBuffer
import scray.querying.sync.OnlineBatchSync
import scray.querying.sync.types.SyncTableBasicClasses.SyncTableRowEmpty
import scala.collection.mutable.ListBuffer
import scray.querying.sync.JobInfo
import com.datastax.driver.core.BatchStatement
import com.datastax.driver.core.querybuilder.Update.Where
import com.datastax.driver.core.querybuilder.Update.Conditions

object CassandraImplementation {
  implicit def genericCassandraColumnImplicit[T](implicit cassImplicit: CassandraPrimitive[T]): DBColumnImplementation[T] = new DBColumnImplementation[T] {
    override def getDBType: String = cassImplicit.cassandraType
    override def fromDBType(value: AnyRef): T = cassImplicit.fromCType(value)
    override def toDBType(value: T): AnyRef = cassImplicit.toCType(value)
  }
}

class OnlineBatchSyncCassandra(dbHostname: String, dbSession: Option[DbSession[Statement, Insert, ResultSet]]) extends OnlineBatchSync {

  // Create or use a given DB session.
  val session = dbSession.getOrElse(new DbSession[Statement, Insert, ResultSet](dbHostname) {
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

  val syncTable = SyncTable("SILIDX", "SyncTable")

  /**
   * Create and register tables for a new job.
   */
  def createNewJob[T <: AbstractRows](job: JobInfo, dataTable: T) {
    this.crateTablesIfNotExists(job, dataTable)

    // Check if table is not locked. 
    // To ensure that tables are locked use lockBatchTable/lockOnlineTable.
    var tableIsLocked = true
    0 to job.numberOfBatcheVersions - 1  foreach { i =>
      tableIsLocked &= this.isBatchTableLocked(job)
      tableIsLocked &= this.isOnlineTableLocked(job)
    }

    if (tableIsLocked) {
      throw new IllegalStateException("One job with the same name is already running")
    }
  }
  
  def startNextBatchJob(job: JobInfo): Boolean = {
    
    this.lockBatchTable(job)

    // Mark next version
    val nextBatchVersion = getNewestBatchVersion(job).get % job.numberOfBatcheVersions
    logger.debug(s"Set next batch version to ${nextBatchVersion}") 
    
    val query = QueryBuilder.update(syncTable.keySpace, syncTable.tableName)
      .`with`(QueryBuilder.set(syncTable.columns.state.name, State.NEXT_JOB.toString()))
      .where(QueryBuilder.eq(syncTable.columns.jobname.name, job.name))
      .and(QueryBuilder.eq(syncTable.columns.online.name, false))
      .and(QueryBuilder.eq(syncTable.columns.versionNr.name, nextBatchVersion))
      .onlyIf(QueryBuilder.eq(syncTable.columns.locked.name, true))

    executeQuorum(query)
  }
  
  def startNextOnlineJob(job: JobInfo): Boolean = {
    
    this.lockOnlineTable(job)

    // Mark next version
    val nextOnlineVersion = getNewestBatchVersion(job).get % job.numberOfOnlineVersions
    logger.debug(s"Set next batch version to ${nextOnlineVersion}") 
    
    val query = QueryBuilder.update(syncTable.keySpace, syncTable.tableName)
      .`with`(QueryBuilder.set(syncTable.columns.state.name, State.NEXT_JOB.toString()))
      .where(QueryBuilder.eq(syncTable.columns.jobname.name, job.name))
      .and(QueryBuilder.eq(syncTable.columns.online.name, true))
      .and(QueryBuilder.eq(syncTable.columns.versionNr.name, nextOnlineVersion))
      .onlyIf(QueryBuilder.eq(syncTable.columns.locked.name, true))

    executeQuorum(query)
  }
  
  def completeBatchJob(job: JobInfo): Boolean = {
    
    // Mark version as completed    
    val runningVersion = getRunningBatchVersion(job)
    
    val query = QueryBuilder.update(syncTable.keySpace, syncTable.tableName)
      .`with`(QueryBuilder.set(syncTable.columns.state.name, State.COMPLETED.toString()))
      .where(QueryBuilder.eq(syncTable.columns.jobname.name, job.name))
      .and(QueryBuilder.eq(syncTable.columns.online.name, false))
      .and(QueryBuilder.eq(syncTable.columns.versionNr.name, runningVersion))
      .onlyIf(QueryBuilder.eq(syncTable.columns.locked.name, true))

    executeQuorum(query)
  }


  /**
   * Check if tables exists and tables are locked
   */
  private def crateTablesIfNotExists[T <: AbstractRows](job: JobInfo, dataColumns: T): Unit = {
    createKeyspace(syncTable)
    syncTable.columns.indexes match {
      case _: Some[List[String]] => createIndexStrings(syncTable).map {session.execute(_)}
      case _                     =>
    }

    // Create data tables and register them in sync table
    0 to job.numberOfBatcheVersions - 1 foreach { i =>

      // Create batch data tables
      session.execute(createSingleTableString(VoidTable(syncTable.keySpace, getBatchJobName(job.name, i), dataColumns)))

      // Register batch table
      session.execute(QueryBuilder.insertInto(syncTable.keySpace, syncTable.tableName)
        .value(syncTable.columns.versionNr.name, i)
        .value(syncTable.columns.online.name, false)
        .value(syncTable.columns.jobname.name, job.name)
        .value(syncTable.columns.locked.name, false)
        .value(syncTable.columns.state.name, State.NEW.toString())
        .value(syncTable.columns.creationTime.name, System.currentTimeMillis())
        .value(syncTable.columns.versions.name, job.numberOfBatcheVersions)
        .value(syncTable.columns.tablename.name, getBatchJobName(syncTable.keySpace + "." + job.name, i))
        .ifNotExists)
    }

    // Create and register online tables
    0 to job.numberOfOnlineVersions - 1 foreach { i =>
      // Create online data tables
      // Columns[Column[_]]
      session.execute(createSingleTableString(VoidTable(syncTable.keySpace, getOnlineJobName(job.name, i), dataColumns)))

      // Register online tables
      session.execute(QueryBuilder.insertInto(syncTable.keySpace, syncTable.tableName)
        .value(syncTable.columns.online.name, true)
        .value(syncTable.columns.jobname.name, job.name)
        .value(syncTable.columns.versionNr.name, i)
        .value(syncTable.columns.versions.name, job.numberOfOnlineVersions)
        .value(syncTable.columns.locked.name, false)
        .value(syncTable.columns.state.name, State.NEW.toString())
        .value(syncTable.columns.creationTime.name, System.currentTimeMillis())
        .value(syncTable.columns.tablename.name, getOnlineJobName(syncTable.keySpace + "." + job.name, i))
        .ifNotExists)
    }
  }

  
  def getNewestBatchVersion(job: JobInfo): Option[Int] = {
    val headBatchQuery: RegularStatement = QueryBuilder.select().from(syncTable.keySpace,  syncTable.tableName).
      where(QueryBuilder.eq(syncTable.columns.jobname.name, job.name))
      .and(QueryBuilder.eq(syncTable.columns.online.name, false))
      
    val headBatches = this.execute(headBatchQuery)
    
    // Find newest version 
    val newestBatchNr = this.getNewestRow(headBatches.iterator()).getInt(syncTable.columns.versionNr.name)
    Option(newestBatchNr)
  }
  
  private def getRunningBatchVersion(job: JobInfo): (Boolean, Int) = {
     
    val versions = execute(QueryBuilder.select().all().from(syncTable.keySpace, syncTable.tableName).where(
      QueryBuilder.eq(syncTable.columns.jobname.name, job.name)).
      and(QueryBuilder.eq(syncTable.columns.online.name, false)).
      and(QueryBuilder.eq(syncTable.columns.state.name, State.RUNNING.toString()))).all()
    
      if(versions.size() > 1) {
        logger.error(s"Inconsistant state. More than one version of job ${job.name} is running")
        (false, 0)
      } else {
        (true, versions.get(0).getInt(syncTable.columns.versionNr.name))
      }
  }
  
  private def getRunningOnlineVersion(job: JobInfo): (Boolean, Int) = {
     
    val versions = execute(QueryBuilder.select().all().from(syncTable.keySpace, syncTable.tableName).where(
      QueryBuilder.eq(syncTable.columns.jobname.name, job.name)).
      and(QueryBuilder.eq(syncTable.columns.online.name, true)).
      and(QueryBuilder.eq(syncTable.columns.state.name, State.RUNNING.toString()))).all()
    
      if(versions.size() > 1) {
        logger.error(s"Inconsistant state. More than one version of job ${job.name} is running")
        (false, 0)
      } else {
        (true, versions.get(0).getInt(syncTable.columns.versionNr.name))
      }
  }
  
  
  
  private def getNewestOnlineVersion(job: JobInfo): Option[Int] = {
    val headBatchQuery: RegularStatement = QueryBuilder.select().from(syncTable.keySpace,  syncTable.tableName).
      where(QueryBuilder.eq(syncTable.columns.jobname.name, job.name))
      .and(QueryBuilder.eq(syncTable.columns.online.name, true))
      
    val headBatches = this.execute(headBatchQuery)

    // Find newest version 
    val newestBatchNr = this.getNewestRow(headBatches.iterator()).getInt(syncTable.columns.versionNr.name)
    Option(newestBatchNr)
  }

  def insertInOnlineTable(job: JobInfo, nr: Int, data: RowWithValue) {
    if (this.lockOnlineTable(job)) {
      val statement = data.foldLeft(QueryBuilder.insertInto(syncTable.keySpace, getOnlineJobName(job.name, nr))) {
        (acc, column) => acc.value(column.name, column.value)
      }
      session.insert(statement)
      this.unlockBatchTable(job)
    } else {
      logger.error(s"Online table for job ${job.name} is locked. It is not possible to insert Data.")
    }
    this.unlockOnlineTable(job)
  }

  def insertInBatchTable(job: JobInfo, nr: Int, data: RowWithValue) {
    if (this.lockBatchTable(job)) {
      val statement = data.foldLeft(QueryBuilder.insertInto(syncTable.keySpace, getBatchJobName(job.name, nr))) {
        (acc, column) => acc.value(column.name, column.value)
      }
      session.insert(statement)
      this.unlockBatchTable(job)
    } else {
      logger.error(s"Online table for job ${job.name} is locked. It is not possible to insert Data.")
    }
  }

  ////  def getTailBatch(jobName: String, session: Session): Option[CassandraTableLocation] = {
  ////    val headBatchQuery: RegularStatement = QueryBuilder.select().from(table.keySpace + "." + table.columnFamily).
  ////      where().and((QueryBuilder.eq(table.columns(0)._1, jobName))).and((QueryBuilder.eq(table.columns(3)._1, false)))
  ////    val headBatches = session.execute(headBatchQuery)
  ////
  ////    // Find newest version 
  ////    val newestBatch = this.getOldestRow(headBatches.iterator())
  ////    Option(CassandraTableLocation(table.keySpace, newestBatch.getString(5)))
  ////  }

  /**
   * Create base keyspace and table
   */
  def createKeyspace[T <: AbstractRows](table: Table[T]): Unit = {
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS ${table.keySpace} WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1};")
    session.execute(createSingleTableString(table))
  }

  /**
   * Lock table if it is used by another spark job.
   */
  def lockOnlineTable(job: JobInfo): Boolean = {
    logger.debug(s"Lock online table for job: ${job.name} ")
    
    val rowsToLock = new BatchStatement()
    0 to job.numberOfOnlineVersions - 1 foreach { 
      version => rowsToLock.add(geLockStatement(job, version, true, true))
    }
    executeQuorum(rowsToLock)
  }

  /**
   * Lock online table if it is used by another spark job.
   */
  def lockBatchTable(job: JobInfo): Boolean = {
    logger.debug(s"Lock batch table for job: ${job.name}")
    
    val rowsToLock = new BatchStatement()
    0 to job.numberOfOnlineVersions - 1 foreach { 
      version => rowsToLock.add(geLockStatement(job, version, false, true))
    }
    executeQuorum(rowsToLock)
  }

  /**
   * Unlock batch table to make it available for a new job.
   */
  def unlockBatchTable(job: JobInfo): Boolean = {
    logger.debug(s"Unlock batch table for job: ${job.name} ")
   
    val rowsToUnlock = new BatchStatement()
    0 to job.numberOfOnlineVersions - 1 foreach { 
      version => rowsToUnlock.add(geLockStatement(job, version, false, false))
    }
    executeQuorum(rowsToUnlock)
  }

  /**
   * Unlock batch table to make it available for a new job.
   */
  def unlockOnlineTable(job: JobInfo): Boolean = {
    logger.debug(s"Unlock online table for job: ${job.name}")
    
    val rowsToUnlock = new BatchStatement()
    0 to job.numberOfOnlineVersions -1 foreach { 
      version => rowsToUnlock.add(geLockStatement(job, version, true, false))
    }
    executeQuorum(rowsToUnlock)
  }
  //  
  //  def getNextBatch(): Option[Int] = {
  //    // Get latest completed batch nr.
  //    
  //    // Return nr + 1 % number of batches.
  //    
  //    None
  //   }
  //
  //
  
  /**
   * Check if online table is locked.
   * To ensure that online table is locked use lockOnlineTable.
   */
  def isOnlineTableLocked(job: JobInfo): Boolean = {
    logger.debug(s"Unlock table for job: ${job.name}")
    val res = execute(QueryBuilder.select().all().from(syncTable.keySpace, syncTable.tableName).where(
      QueryBuilder.eq(syncTable.columns.jobname.name, job.name)).
      and(QueryBuilder.eq(syncTable.columns.online.name, true)).
      and(QueryBuilder.eq(syncTable.columns.locked.name, true)))
    (res.all().size() > 0)
  }

    /**
   * Check if batch table is locked.
   * To ensure that batch table is locked use lockBatchTable.
   */
  def isBatchTableLocked(job: JobInfo): Boolean = {
    logger.debug(s"Unlock table for job: ${job.name}")
    val res = execute(QueryBuilder.select().all().from(syncTable.keySpace, syncTable.tableName).
      where(QueryBuilder.eq(syncTable.columns.jobname.name, job.name)).
      and(QueryBuilder.eq(syncTable.columns.online.name, false)).
      and(QueryBuilder.eq(syncTable.columns.locked.name, true)))
    (res.all().size() > 0)
  }

  def geLockStatement(job: JobInfo, version: Int, online: Boolean, newState: Boolean):  Conditions = {
     QueryBuilder.update(syncTable.keySpace, syncTable.tableName).
      `with`(QueryBuilder.set(syncTable.columns.locked.name, newState)).
      where(QueryBuilder.eq(syncTable.columns.jobname.name, job.name)).
      and(QueryBuilder.eq(syncTable.columns.online.name, online)).
      and(QueryBuilder.eq(syncTable.columns.versionNr.name, version)).
      onlyIf(QueryBuilder.eq(syncTable.columns.locked.name, !newState))
  }

  //  def getSynctable(jobName: String): Option[Table[Columns[ColumnV[_]]]] = {
  //    val rows = execute(QueryBuilder.select().all().from(syncTable.keySpace, syncTable.tableName).where(QueryBuilder.eq(syncTable.columns.jobname.name, jobName)))
  //    None
  //  }

  def getOnlineJobData[T <: RowWithValue](jobname: String, nr: Int, result: T): Option[List[RowWithValue]] = {
    def handleColumnWithValue[U](currentRow: Row, destinationColumn: ColumnWithValue[U]): U = {
      val dbimpl = destinationColumn.dbimpl
      dbimpl.fromDBType(currentRow.get(destinationColumn.name, dbimpl.toDBType(destinationColumn.value).getClass()))
    }

    def fillValue[U](currentRow: Row, destinationColumn: ColumnWithValue[U]) = {
      destinationColumn.value = handleColumnWithValue(currentRow, destinationColumn)
    }

    val rows = execute(QueryBuilder.select().all().from(syncTable.keySpace, getOnlineJobName(jobname, nr)))
    val dbDataIter = rows.iterator()

    var columns = new ListBuffer[RowWithValue]()

    if (dbDataIter.hasNext()) {
      while (dbDataIter.hasNext()) {
        val nextRow = result.copy()
        nextRow.columns.map { destinationColumn =>
          fillValue(dbDataIter.next(), destinationColumn)
        }
        columns += nextRow
      }

      Some(columns.toList)

      //Option(SumDataColumns(column.getLong(sumDataColumns.time.name), column.getLong(sumDataColumns.sum.name)))
    } else {
      logger.error(s"No data for job ${jobname} ${nr} found")
      None
    }
  }

  //  def purgeAllTables() = {
  //    
  //    session.execute(s"DROP KEYSPACE ${syncTable.keySpace}")
  //  }
  //  
  //  def getSyncTable: Table[SyncTableColumnsValues[IndexedSeq[_]]] = {
  //    val rowsIter = execute(QueryBuilder.select().all().from(syncTable.keySpace, syncTable.tableName)).iterator()
  //    
  //     val nrV = new ArrayBuffer[Int]()
  //     val jobnameV = new ArrayBuffer[String]()
  //     val timeV = new ArrayBuffer[Long]()
  //     val lockV = new ArrayBuffer[Boolean]()
  //     val onlineV = new ArrayBuffer[Boolean]()
  //     val completedV = new ArrayBuffer[Boolean]()
  //     val tablenameV = new ArrayBuffer[String]()
  //     val batchesV = new ArrayBuffer[Int]()
  //     val onlineVersionsV = new ArrayBuffer[Int]()
  //     val stateV = new ArrayBuffer[String]()
  //     
  //     while(rowsIter.hasNext()) {
  //      val row = rowsIter.next()
  //      
  //      nrV.+=(row.getInt(syncTable.columns.nr.name))
  //      jobnameV += row.getString(syncTable.columns.jobname.name)
  //      timeV += row.getLong(syncTable.columns.time.name)
  //      lockV += row.getBool(syncTable.columns.lock.name)
  //      onlineV += row.getBool(syncTable.columns.online.name)
  //      completedV += row.getBool(syncTable.columns.completed.name)
  //      tablenameV += row.getString(syncTable.columns.tablename.name)
  //      batchesV += row.getInt(syncTable.columns.batches.name)
  //      onlineVersionsV += row.getInt(syncTable.columns.onlineVersions.name)
  //      stateV += row.getString(syncTable.columns.state.name)
  //    }
  //     
  //    val columnsA = new SyncTableColumnsValuesS(nrV, jobnameV, timeV, lockV, onlineV, completedV, tablenameV, batchesV, onlineVersionsV, stateV)
  //    
  //    class Result extends Table[SyncTableColumnsValues[_]](keySpace = "", tableName = "\"SyncTable\"", columns = columnsA)
  //    
  //    return new Result
  //   }
  ////    println(rows.getColumnDefinitions)
  ////    val iter = rows.all().iterator()
  ////    while(iter.hasNext()) {
  ////      println(iter.next())
  ////    }
  //    
  ////    val result = new SyncTableColumnsValues(
  ////        nrV, 
  ////        jobnameV, 
  ////        timeV, 
  ////        lockV, 
  ////        onlineV,
  ////        completedV, 
  ////        tablenameV, 
  ////        batchesV, 
  ////        onlineVersionsV, 
  ////        stateV)
  //      val result = new SyncTableColumnsValues[List[_]](
  //        2, 
  //        "Na", 
  //        1L, 
  //        false, 
  //        false,
  //        false, 
  //        "t1", 
  //        1, 
  //        1, 
  //        "SART")
  //      val r2 = 
  //     r2
  //  }
  private def executeQuorum(statement: Statement): Boolean = {
    val a = new BatchStatement
    a.getStatements
    
    statement match {
      case bStatement: BatchStatement =>  logger.debug("Execute batch statement: " + bStatement.getStatements)
      case _ =>                            logger.debug("Execute query: " + statement)
    }
    
    statement.setConsistencyLevel(ConsistencyLevel.QUORUM)

    val rs = session.execute(statement);
    val row = rs.one();

    if (row.getBool("[applied]")) {
      logger.debug(s"Execute ${statement}");
      true
    } else {
      statement match {
        case bStatement: BatchStatement =>  logger.error("It is currently not possible to execute : " + bStatement.getStatements)
        case _ =>                           logger.error("It is currently not possible to execute : " + statement)
       }
      false
    }
  }

  private def execute(statement: RegularStatement) = {
    logger.debug("Execute: " + statement)
    statement.setConsistencyLevel(ConsistencyLevel.QUORUM);

    session.execute(statement);
  }

  def createSingleTableString[T <: AbstractRows](table: Table[T]): String = {
    val createStatement = s"CREATE TABLE IF NOT EXISTS ${table.keySpace + "." + table.tableName} (" +
      s"${table.columns.foldLeft("")((acc, next) => { acc + next.name + " " + next.getDBType + ", " })} " +
      s"PRIMARY KEY ${table.columns.primaryKey})"
    logger.debug(s"Create table String: ${createStatement} ")
    createStatement
  }

  private def createIndexStrings[T <: AbstractRows](table: Table[T]): List[String] = {
    
    def addString(column: String): String = {
      s"CREATE INDEX IF NOT EXISTS ON ${table.keySpace}.${table.tableName} (${column})"
    }
    
    table.columns.indexes.getOrElse(List("")).foldLeft(List[String]())((acc, indexStatement) => addString(indexStatement) :: acc) 
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
      if (nextRows.hasNext) {
        val localRow = nextRows.next()
        logger.debug(s"Work with row ${localRow}")
        val max = if (comp(prevRow.getLong(syncTable.columns.creationTime.name), localRow.getLong(syncTable.columns.creationTime.name))) {
          prevRow
        } else {
          localRow
        }
        accNewestRow(max, nextRows)
      } else {
        logger.debug(s"Return newest row ${prevRow}")
        prevRow
      }
    }
    import scala.collection.convert.WrapAsScala.asScalaIterator

    val row = rows.next()
    logger.debug(s"Inspect new row ${rows.size}")
    accNewestRow(row, rows)
  }
}