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
import scray.querying.sync.types.State.State
import com.datastax.driver.core.querybuilder.Update.Where
import com.datastax.driver.core.querybuilder.Update.Conditions
import scala.util.Try
import scray.querying.sync.RunningJobExistsException
import scray.querying.sync.NoRunningJobExistsException
import scray.querying.sync.StatementExecutionError
import scala.util.Failure
import scala.util.Success
import scray.querying.description.TableIdentifier
import scala.collection.mutable.HashSet

object CassandraImplementation {
  implicit def genericCassandraColumnImplicit[T](implicit cassImplicit: CassandraPrimitive[T]): DBColumnImplementation[T] = new DBColumnImplementation[T] {
    override def getDBType: String = cassImplicit.cassandraType
    override def fromDBType(value: AnyRef): T = cassImplicit.fromCType(value)
    override def toDBType(value: T): AnyRef = cassImplicit.toCType(value)
  }

  implicit class RichBoolean(val b: Boolean) extends AnyVal {
    final def toOption[A](a: => A): Option[A] = if (b) Some(a) else None
    final def toTry[A, E <: Throwable](a: => A, c: => E): Try[A] = Try { if (b) a else throw c }
    final def ?[A](a: => A, c: => A): A = if (b) a else c
    final def ?[A](a: => A) = if (b) a
    final def ![A](a: => A) = if (!b) a
  }

  implicit class RichOption[T](val b: Option[T]) extends AnyVal {
    final def toTry[E <: Throwable](c: => E): Try[T] = Try { b.getOrElse(throw c) }
  }
}

class CassandraSessionBasedDBSession(cassandraSession: Session) extends DbSession[Statement, Insert, ResultSet](cassandraSession.getCluster.getMetadata.getAllHosts().iterator().next.getAddress.toString) {

    override def execute(statement: String): Try[ResultSet] = {
      val result = cassandraSession.execute(statement)
      if(result.wasApplied()) {
        Success(result)
      } else {
        Failure(new StatementExecutionError(s"It was not possible to execute statement: ${statement}. Error: ${result.getExecutionInfo}"))
      }
    }

    def execute(statement: Statement): Try[ResultSet] = {
      val result = cassandraSession.execute(statement)
      if(result.wasApplied()) {
        Success(result)
      } else {
        Failure(new StatementExecutionError(s"It was not possible to execute statement: ${statement}. Error: ${result.getExecutionInfo}"))
      }
    }

    def insert(statement: Insert): Try[ResultSet] = {
      val result = cassandraSession.execute(statement)
      if(result.wasApplied()) {
        Success(result)
      } else {
        Failure(new StatementExecutionError(s"It was not possible to execute statement: ${statement}. Error: ${result.getExecutionInfo}"))
      }
    }

    def execute(statement: SimpleStatement): Try[ResultSet] = {
      val result = cassandraSession.execute(statement)
      if(result.wasApplied()) {
        Success(result)
      } else {
        Failure(new StatementExecutionError(s"It was not possible to execute statement: ${statement}. Error: ${result.getExecutionInfo}"))
      }
    }
  }


class OnlineBatchSyncCassandra(dbSession: DbSession[Statement, Insert, ResultSet]) extends OnlineBatchSync {

  def this(dbHostname: String) = {
    this(new CassandraSessionBasedDBSession(Cluster.builder().addContactPoint(dbHostname).build().connect()))
  }
  
  import CassandraImplementation.{RichBoolean, RichOption}
  
  // Create or use a given DB session.
  val session = dbSession

  val syncTable = SyncTable("SILIDX", "SyncTable")

  /**
   * Create and register tables for a new job.
   */
  def initJob[T <: AbstractRow](job: JobInfo, dataTable: T): Try[Unit] = {
    this.crateTablesIfNotExists(job, dataTable)

    // Check if table is not locked. 
    // To ensure that tables are locked use lockBatchTable/lockOnlineTable.
    val batchLocked = this.isBatchTableLocked(job)
    val onlineLocked = this.isOnlineTableLocked(job)

    if(batchLocked.isDefined && onlineLocked.isDefined) {
      if(!(!batchLocked.get && !onlineLocked.get)) {
        Failure(new IllegalStateException("One job with the same name is already running"))
      } else {
        Try()
      }
    } else {
      Failure(new StatementExecutionError(""))
    }
  }

  def startNextBatchJob(job: JobInfo): Try[Unit] = {
    this.startJob(job, false)
  }

  def startNextOnlineJob(job: JobInfo): Try[Unit] = {
    this.startJob(job, true)
  }

  private def startJob(job: JobInfo, online: Boolean): Try[Unit] = {
    def createStartStatement(version: Int, online: Boolean): Statement = {
      QueryBuilder.update(syncTable.keySpace, syncTable.tableName)
        .`with`(QueryBuilder.set(syncTable.columns.state.name, State.RUNNING.toString()))
        .where(QueryBuilder.eq(syncTable.columns.jobname.name, job.name))
        .and(QueryBuilder.eq(syncTable.columns.online.name, online))
        .and(QueryBuilder.eq(syncTable.columns.versionNr.name, version))
        .onlyIf(QueryBuilder.eq(syncTable.columns.locked.name, true))
    }
    if (online) {
      // Abort if a job is already running
      this.getRunningOnlineJobVersion(job) match {
        case Some(version) => {
          logger.error(s"Job ${job.name} with version ${version} is currently running")
          Failure(new RunningJobExistsException(s"Online job ${job.name} with version ${version} is currently running"))
        }
        case None => {
          this.lockOnlineTable(job).map { _ =>
          val newVersion = getNewestOnlineVersion(job)
          .getOrElse({
            logger.debug("No completed version found use 0"); 
            0}) + 1 % job.numberOfOnlineVersions 
            logger.debug(s"Set next batch version to ${newVersion}")
            executeQuorum(createStartStatement(newVersion, true))
          }
        }
      }
    } else {
      // Abort if a job is already running
      this.getRunningBatchJobVersion(job) match {
        case version: Some[Int] => {
          logger.error(s"Job ${job.name} with version ${version} is currently running")
          Failure(new RunningJobExistsException(s"Batch job ${job.name} with version ${version} is currently running"))
        }
        case _ =>
          this.lockBatchTable(job).map { _ =>
          getNewestBatchVersion(job)
            .map { _ + 1 % job.numberOfBatcheVersions }
            .map { newVersion =>
              logger.debug(s"Set next batch version to ${newVersion}")
              createStartStatement(newVersion, false)
            }
            .map { statement => executeQuorum(statement) }
          }
      }
    }
  }

  def completeBatchJob(job: JobInfo): Try[Unit] = Try {
    getRunningBatchJobVersion(job) match {
      case version: Some[Int] =>
        this.completeJob(job, version.get, false)
      case None               => throw new NoRunningJobExistsException(s"No running job with name: ${job.name} exists.")
    }
  }

  def completeOnlineJob(job: JobInfo): Try[Unit] = Try {
    getRunningOnlineJobVersion(job) match {
      case version: Some[Int] =>
        this.completeJob(job, version.get, true); 
      case None               => throw new NoRunningJobExistsException(s"No running job with name: ${job.name} exists.")
    }
  }

  override def getQueryableTableIdentifiers: List[(String, TableIdentifier, Int)] = {
    def splitIntoTableIdentifier(tableId: String): TableIdentifier = {
      val parts = tableId.split("\\.").reverse
      val tablename = parts(0)
      val keyspace = if(parts.size > 1) {
        parts(1)
      } else {
        syncTable.keySpace
      }
      val system = if(parts.size > 2) {
        parts(2)
      } else {
        "cassandra"
      }
      TableIdentifier(system, keyspace, tablename)
    }
    import scala.collection.convert.decorateAsScala.asScalaIteratorConverter
    val jobnames = new HashSet[String]()
    val query = QueryBuilder.select(syncTable.columns.jobname.name).from(syncTable.keySpace, syncTable.tableName)
    val results = execute(query)
    results.map { resultset => 
      resultset.iterator.asScala.map { row =>
        val currJob = row.getString(syncTable.columns.jobname.name)
        jobnames(currJob) match {
          case true => None
          case false => 
            jobnames += currJob
            val (version, tablename) =  getNewestVersionAndTable(JobInfo(currJob), true).getOrElse {
              getNewestVersionAndTable(JobInfo(currJob), false).getOrElse {
                (-1, "")
              }
            }
            if(version > -1) {
              Some((currJob, splitIntoTableIdentifier(tablename), version))
            } else {
              None
            }
        }
        // row.getString(syncTable.columns.tableidentifier.name).split("\\.")
      }.filter(_.isDefined).map(_.get).toList
    }.getOrElse(List())
  }
  
  private def completeJob(job: JobInfo, version: Int, online: Boolean): Try[Unit] = {
    val query = QueryBuilder.update(syncTable.keySpace, syncTable.tableName)
      .`with`(QueryBuilder.set(syncTable.columns.state.name, State.COMPLETED.toString()))
      .and(QueryBuilder.set(syncTable.columns.creationTime.name, System.currentTimeMillis()))
      .where(QueryBuilder.eq(syncTable.columns.jobname.name, job.name))
      .and(QueryBuilder.eq(syncTable.columns.online.name, online))
      .and(QueryBuilder.eq(syncTable.columns.versionNr.name, version))
      .onlyIf(QueryBuilder.eq(syncTable.columns.locked.name, true))
    executeQuorum(query)
  }

  def getRunningBatchJobVersion(job: JobInfo): Option[Int] = {
    this.getRunningVersion(job, false)
  }

  def getRunningOnlineJobVersion(job: JobInfo): Option[Int] = {
    this.getRunningVersion(job, true)
  }

  private def getRunningVersion(job: JobInfo, online: Boolean): Option[Int] = {

    val versions = execute(QueryBuilder.select().all().from(syncTable.keySpace, syncTable.tableName).where(
      QueryBuilder.eq(syncTable.columns.jobname.name, job.name)).
      and(QueryBuilder.eq(syncTable.columns.online.name, online)).
      and(QueryBuilder.eq(syncTable.columns.state.name, State.RUNNING.toString()))).map {_.all()}
    
    if(versions.isSuccess) {
      if (versions.get.size() > 1) {
        logger.error(s"Inconsistant state. More than one version of job ${job.name} are running")
        None
      } else {
        if(versions.get.size() == 0) {
          None
        } else {
          Some(versions.get.get(0).getInt(syncTable.columns.versionNr.name))
        }
      }
    } else {
      None
    }
  }

  /**
   * Check if tables exists and tables are locked
   */
  private def crateTablesIfNotExists[T <: AbstractRow](job: JobInfo, dataColumns: T): Try[Unit] = {
    createKeyspace(syncTable)
    syncTable.columns.indexes match {
      case _: Some[List[String]] => createIndexStrings(syncTable).map { session.execute(_) }
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
        .value(syncTable.columns.tableidentifier.name, getBatchJobName(syncTable.keySpace + "." + job.name, i))
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
        .value(syncTable.columns.tableidentifier.name, getOnlineJobName(syncTable.keySpace + "." + job.name, i))
        .ifNotExists)
    }
    Try()
  }

  def getOnlineJobState(job: JobInfo, version: Int): Option[State] = {
    this.getJobState(job, version, true)
  }

  def getBatchJobState(job: JobInfo, version: Int): Option[State] = {
    this.getJobState(job, version, false)
  }
  
  private def getJobState(job: JobInfo, version: Int, online: Boolean) : Option[State] = {
    execute(QueryBuilder.select(syncTable.columns.state.name).from(syncTable.keySpace, syncTable.tableName).where(
      QueryBuilder.eq(syncTable.columns.jobname.name, job.name)).
      and(QueryBuilder.eq(syncTable.columns.online.name, online)).
      and(QueryBuilder.eq(syncTable.columns.versionNr.name, version)))
      .map { resultset => resultset.all().get(0).getString(0) }
      .map { state => State.values.find(_.toString() == state).get }
      .toOption
  }

  def getNewestOnlineVersion(job: JobInfo): Option[Int] = getNewestVersionAndTable(job, true).map(_._1)
  def getNewestBatchVersion(job: JobInfo): Option[Int] = getNewestVersionAndTable(job, false).map(_._1)

  private def getNewestVersionAndTable(job: JobInfo, online: Boolean): Option[(Int, String)] = {
    val headBatchQuery: RegularStatement = QueryBuilder.select().from(syncTable.keySpace, syncTable.tableName).
      where(QueryBuilder.eq(syncTable.columns.jobname.name, job.name))
      .and(QueryBuilder.eq(syncTable.columns.online.name, online))
      .and(QueryBuilder.eq(syncTable.columns.state.name, State.COMPLETED.toString()))

    // Find newest version
    this.execute(headBatchQuery)
      .map { _.iterator() }
      .map { iter => this.getNewestRow(iter) }
      .map { row => (row.getInt(syncTable.columns.versionNr.name), row.getString(syncTable.columns.tableidentifier.name)) }
      .toOption
  }


  
  def insertInOnlineTable(job: JobInfo, version: Int, data: RowWithValue): Try[Unit] = {
    val statement = data.foldLeft(QueryBuilder.insertInto(syncTable.keySpace, getOnlineJobName(job.name, version))) {
      (acc, column) => acc.value(column.name, column.value)
    }

    logger.debug(s"Insert data in online table ${job.name} for version ${version}: ${statement}")
    session.insert(statement) match {
      case Success(_) => Try()
      case Failure(message) => Failure(this.throwInsertStatementError(job.name, version, statement, message.toString()))
    }
  }

  private def throwInsertStatementError(jobname: String, version: Int, statement: Statement, message: String) = {
    throw new StatementExecutionError(s"Error while inserting data for job ${jobname}, version ${version}. Statement: ${statement}. ${message}")
  }
  
  def insertInBatchTable(job: JobInfo, version: Int, data: RowWithValue): Try[Unit] = {
    val statement = data.foldLeft(QueryBuilder.insertInto(syncTable.keySpace, getBatchJobName(job.name, version))) {
      (acc, column) => acc.value(column.name, column.value)
    }

    session.insert(statement) match {
      case Success(_) => Try()
      case Failure(message) => Failure(this.throwInsertStatementError(job.name, version, statement, message.toString()))
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
  def createKeyspace[T <: AbstractRow](table: Table[T]): Try[Unit] = {
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS ${table.keySpace} WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1};")
    session.execute(createSingleTableString(table))
    
//      statement match {
//        case bStatement: BatchStatement => logger.error("It is currently not possible to execute : " + bStatement.getStatements)
//        case _                          => logger.error("It is currently not possible to execute : " + statement)
//      }
     Try()
  }

  /**
   * Lock table if it is used by another spark job.
   */
  def lockOnlineTable(job: JobInfo): Try[Unit] = {
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
  def lockBatchTable(job: JobInfo): Try[Unit] = {
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
  def unlockBatchTable(job: JobInfo): Try[Unit] = {
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
  def unlockOnlineTable(job: JobInfo): Try[Unit] = {
    logger.debug(s"Unlock online table for job: ${job.name}")

    val rowsToUnlock = new BatchStatement()
    0 to job.numberOfOnlineVersions - 1 foreach {
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
    def isOnlineTableLocked(job: JobInfo): Option[Boolean] = {
       logger.debug(s"Check if online table is locked for job: ${job.name}")
       isTableLocked(job, true)
    }
  
      /**
     * Check if batch table is locked.
     * To ensure that batch table is locked use lockBatchTable.
     */
    def isBatchTableLocked(job: JobInfo): Option[Boolean] = {
       logger.debug(s"Check if batch table is locked for job: ${job.name}")
       isTableLocked(job, false)
    }
    
    private def isTableLocked(job: JobInfo, online: Boolean): Option[Boolean] = {
      val res = execute(QueryBuilder.select().all().from(syncTable.keySpace, syncTable.tableName).where(
        QueryBuilder.eq(syncTable.columns.jobname.name, job.name)).
        and(QueryBuilder.eq(syncTable.columns.online.name, online)).
        and(QueryBuilder.eq(syncTable.columns.locked.name, true)))
        res.map { rows => rows.all().size() > 0 }.toOption
    }

  def geLockStatement(job: JobInfo, version: Int, online: Boolean, newState: Boolean): Conditions = {
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

  def getBatchJobData[T <: RowWithValue](jobname: String, version: Int, result: T): Option[List[RowWithValue]] = {
    getJobData(jobname, version, false, result)
  }
  def getOnlineJobData[T <: RowWithValue](jobname: String, version: Int, result: T): Option[List[RowWithValue]] = {
    getJobData(jobname, version, true, result)
  }

  private def getJobData[T <: RowWithValue](jobname: String, version: Int, online: Boolean, result: T): Option[List[RowWithValue]] = {
    def handleColumnWithValue[U](currentRow: Row, destinationColumn: ColumnWithValue[U]): U = {
      val dbimpl = destinationColumn.dbimpl
      dbimpl.fromDBType(currentRow.get(destinationColumn.name, dbimpl.toDBType(destinationColumn.value).getClass()))
    }

    def fillValue[U](currentRow: Row, destinationColumn: ColumnWithValue[U]) = {
      destinationColumn.value = handleColumnWithValue(currentRow, destinationColumn)
    }

    val statementResult = online match {
      case true => execute(QueryBuilder.select().all().from(syncTable.keySpace, getOnlineJobName(jobname, version))).map { x => x.iterator() }
      case _    => execute(QueryBuilder.select().all().from(syncTable.keySpace, getBatchJobName(jobname, version))).map { x => x.iterator() }
    }
    var columns = new ListBuffer[RowWithValue]()

    if (statementResult.isFailure) {
      logger.error(s"No data for job ${jobname} ${version} found")
      None
    } else {
      val dbDataIter = statementResult.get
      if (dbDataIter.hasNext()) {
        while (dbDataIter.hasNext()) {
          val nextRow = result.copy()
          nextRow.columns.map { destinationColumn =>
            fillValue(dbDataIter.next(), destinationColumn)
          }
          columns += nextRow
        }
        Some(columns.toList)
      } else {
        None
      }
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
  
  private def executeQuorum(statement: Statement): Try[Unit] = {

    statement match {
      case bStatement: BatchStatement => logger.debug("Execute batch statement: " + bStatement.getStatements)
      case _                          => logger.debug("Execute query: " + statement)
    }
    statement.setConsistencyLevel(ConsistencyLevel.QUORUM)

    session.execute(statement) match {
      case Success(result) => Try()
      case Failure(ex) => {
          logger.error(s"Error while executing statement: ${statement}. ${ex.printStackTrace()}")
          Try(new StatementExecutionError(ex.getLocalizedMessage))
        } 
    }
  }

  private def execute(statement: RegularStatement): Try[ResultSet] = {
    logger.debug("Execute: " + statement)
    statement.setConsistencyLevel(ConsistencyLevel.QUORUM);

    session.execute(statement) match {
      case Success(result) => Try(result)
      case Failure(ex) => {
          logger.error(s"Error while executing statement: ${statement}. ${ex.printStackTrace()}")
          Try(throw new StatementExecutionError(ex.getLocalizedMessage))
        } 
    }
  }

  def createSingleTableString[T <: AbstractRow](table: Table[T]): String = {
    val createStatement = s"CREATE TABLE IF NOT EXISTS ${table.keySpace + "." + table.tableName} (" +
      s"${table.columns.foldLeft("")((acc, next) => { acc + next.name + " " + next.getDBType + ", " })} " +
      s"PRIMARY KEY ${table.columns.primaryKey})"
    logger.debug(s"Create table String: ${createStatement} ")
    createStatement
  }

  private def createIndexStrings[T <: AbstractRow](table: Table[T]): List[String] = {

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