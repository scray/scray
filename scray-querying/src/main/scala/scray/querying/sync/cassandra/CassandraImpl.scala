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
import com.datastax.driver.core.querybuilder.Select

object CassandraImplementation extends Serializable {
  implicit def genericCassandraColumnImplicit[T](implicit cassImplicit: CassandraPrimitive[T]): DBColumnImplementation[T] = new DBColumnImplementation[T] {
    override def getDBType: String = cassImplicit.cassandraType
    override def fromDBType(value: AnyRef): T = cassImplicit.fromCType(value)
    override def toDBType(value: T): AnyRef = cassImplicit.toCType(value)
  }

  implicit class RichBoolean(val b: Boolean) extends AnyVal with Serializable {
    final def toOption[A](a: => A): Option[A] = if (b) Some(a) else None
    final def toTry[A, E <: Throwable](a: => A, c: => E): Try[A] = Try { if (b) a else throw c }
    final def ?[A](a: => A, c: => A): A = if (b) a else c
    final def ?[A](a: => A) = if (b) a
    final def ![A](a: => A) = if (!b) a
  }

  implicit class RichOption[T](val b: Option[T]) extends AnyVal with Serializable {
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
        Failure(new StatementExecutionError(s"It was not possible to execute statement: ${printStatement(statement)}. Error: ${result.getExecutionInfo}"))
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
    
    def printStatement(s: Statement): String = {
      s match {
         case bStatement: BatchStatement => "It is currently not possible to execute : " + bStatement.getStatements
         case _                          => "It is currently not possible to execute : " + s
      }
    }
  }


class OnlineBatchSyncCassandra(dbSession: DbSession[Statement, Insert, ResultSet]) extends OnlineBatchSync {

  def this(dbHostname: String) = {
    this(new CassandraSessionBasedDBSession(Cluster.builder().addContactPoint(dbHostname).build().connect()))
  }
  
  def this(dbHostname: String, port: Int) = {
    this(new CassandraSessionBasedDBSession(Cluster.builder().addContactPoint(dbHostname).withPort(port).build().connect()))
  }
  
  import CassandraImplementation.{RichBoolean, RichOption}
  
  // Create or use a given DB session.
  @transient val session = dbSession

  val syncTable = SyncTable("SILIDX", "SyncTable")
  val jobLockTable = JobLockTable("SILIDX", "JobLockTable")

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
    logger.debug(s"Start next batch job ${job.name}")
    this.startJob(job, false)
  }

  def startNextOnlineJob(job: JobInfo): Try[Unit] = {
    this.startJob(job, true)
  }

  private def startJob(job: JobInfo, online: Boolean): Try[Unit] = {
    def createStartStatement(slot: Int, online: Boolean): Statement = {
      QueryBuilder.update(syncTable.keySpace, syncTable.tableName)
        .`with`(QueryBuilder.set(syncTable.columns.state.name, State.RUNNING.toString()))
        .where(QueryBuilder.eq(syncTable.columns.jobname.name, job.name))
        .and(QueryBuilder.eq(syncTable.columns.online.name, online))
        .and(QueryBuilder.eq(syncTable.columns.slot.name, slot))
        .onlyIf(QueryBuilder.eq(syncTable.columns.locked.name, true))
    }
    if (online) {
      // Abort if a job is already running
      this.getRunningOnlineJobSlot(job) match {
        case Some(slot) => {
          logger.error(s"Job ${job.name} is currently running on slot ${slot}")
          Failure(new RunningJobExistsException(s"Online job ${job.name} slot ${slot} is currently running"))
        }
        case None => {
          this.lockOnlineTable(job).map { _ =>
          val slot = (getNewestOnlineSlot(job)
          .getOrElse({
            logger.debug("No completed slot found use 0"); 
            0}) + 1) % job.numberOfOnlineSlots 
            logger.debug(s"Set next online slot to ${slot}")
            executeQuorum(createStartStatement(slot, true))
          }
        }
      }
    } else {
      // Abort if a job is already running
      this.getRunningBatchJobSlot(job) match {
        case slot: Some[Int] => {
          logger.error(s"Job ${job.name} is currently running on slot ${slot}")
          Failure(new RunningJobExistsException(s"Batch job ${job.name} with slot ${job.batchID} is currently running"))
        }
        case _ =>
          this.lockBatchTable(job).map { _ =>
          (getNewestBatchSlot(job)
          .getOrElse( {logger.debug("No completed slot found use default slot 0"); 0})
          + 1) % job.numberOfBatchSlots }
          .map { newSlot =>
              logger.debug(s"Set next batch slot to ${newSlot}")
              createStartStatement(newSlot, false)
            }
            .map { statement => executeQuorum(statement) }
          }
      }
    }

  def completeBatchJob(job: JobInfo): Try[Unit] = Try {
    getRunningBatchJobSlot(job) match {
      case slot: Some[Int] =>
        this.completeJob(job, slot.get, false)
        .map { _ => this.unlockBatchTable(job)}.flatten
      case None               => throw new NoRunningJobExistsException(s"No running job with name: ${job.name} exists.")
    }
  }

  def completeOnlineJob(job: JobInfo): Try[Unit] = Try {
    getRunningOnlineJobSlot(job) match {
      case slot: Some[Int] =>
        this.completeJob(job, slot.get, true)
        .map { _ => this.unlockOnlineTable(job)}.flatten
      case None               => throw new NoRunningJobExistsException(s"No running job with name: ${job.name} exists.")
    }
  }
  
  def resetBatchJob(job: JobInfo): Try[Unit] = Try {
    
    val runningBatchSlot = this.getRunningBatchJobSlot(job).getOrElse(0)
    this.unlockBatchTable(job)
    this.renewJob(job, runningBatchSlot, false)
    this.lockBatchTable(job)
  }
  
  def resetOnlineJob(job: JobInfo): Try[Unit] = Try {
    
    val runningOnlineSlot = this.getRunningOnlineJobSlot(job).getOrElse(0)
    this.unlockOnlineTable(job)
    this.renewJob(job, runningOnlineSlot, true)
    this.lockOnlineTable(job)
  }
  
  /**
   * Set running job to state new
   */
  private def renewJob(job: JobInfo, slot: Int, online: Boolean): Try[Unit] = {
    val query = QueryBuilder.update(syncTable.keySpace, syncTable.tableName)
      .`with`(QueryBuilder.set(syncTable.columns.state.name, State.NEW.toString()))
      .and(QueryBuilder.set(syncTable.columns.latestUpdate.name, System.currentTimeMillis()))
      .where(QueryBuilder.eq(syncTable.columns.jobname.name, job.name))
      .and(QueryBuilder.eq(syncTable.columns.online.name, online))
      .and(QueryBuilder.eq(syncTable.columns.slot.name, slot))
      .onlyIf(QueryBuilder.eq(syncTable.columns.locked.name, false))
    executeQuorum(query)
  }
    
  private def completeJob(job: JobInfo, slot: Int, online: Boolean): Try[Unit] = {
    val query = QueryBuilder.update(syncTable.keySpace, syncTable.tableName)
      .`with`(QueryBuilder.set(syncTable.columns.state.name, State.COMPLETED.toString()))
      .and(QueryBuilder.set(syncTable.columns.latestUpdate.name, System.currentTimeMillis()))
      .where(QueryBuilder.eq(syncTable.columns.jobname.name, job.name))
      .and(QueryBuilder.eq(syncTable.columns.online.name, online))
      .and(QueryBuilder.eq(syncTable.columns.slot.name, slot))
      .onlyIf(QueryBuilder.eq(syncTable.columns.locked.name, true))
    executeQuorum(query)
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
            val (slot, tablename) =  getNewestSlotAndTable(currJob, true).getOrElse {
              getNewestSlotAndTable(currJob, false).getOrElse {
                (-1, "")
              }
            }
            if(slot > -1) {
              Some((currJob, splitIntoTableIdentifier(tablename), slot))
            } else {
              None
            }
        }
        // row.getString(syncTable.columns.tableidentifier.name).split("\\.")
      }.filter(_.isDefined).map(_.get).toList
    }.getOrElse(List())
  }
  
  def getRunningBatchJobSlot(job: JobInfo): Option[Int] = {
    this.getRunningSlot(job, false)
  }
  
  def getLatestBatch(job: JobInfo): Option[Int] = {
    val slotQuery = QueryBuilder.select(syncTable.columns.batchEndTime.name).from(syncTable.keySpace, syncTable.tableName).where(
      QueryBuilder.eq(syncTable.columns.jobname.name, job.name)).
      and(QueryBuilder.eq(syncTable.columns.online.name, false)).
      and(QueryBuilder.eq(syncTable.columns.state.name, State.COMPLETED.toString()))
    
  this.execute(slotQuery)
      .map { _.iterator() }.recover {
        case e => {logger.error(s"DB error while fetching newest slot ${e.getMessage}"); throw e}
      }.toOption
      .flatMap { iter => this.getNewestRow(iter, syncTable.columns.batchEndTime.name)}
      .map { row => (row.getInt(syncTable.columns.slot.name))} 
  }


  def getRunningOnlineJobSlot(job: JobInfo): Option[Int] = {
    this.getRunningSlot(job, true)
  }

  private def getRunningSlot(job: JobInfo, online: Boolean): Option[Int] = {

    val slots = execute(QueryBuilder.select().all().from(syncTable.keySpace, syncTable.tableName).allowFiltering().where(
      QueryBuilder.eq(syncTable.columns.jobname.name, job.name)).
      and(QueryBuilder.eq(syncTable.columns.online.name, online)).
      and(QueryBuilder.eq(syncTable.columns.batchStartTime.name, job.batchID.getBatchStart)).
      and(QueryBuilder.eq(syncTable.columns.batchEndTime.name, job.batchID.getBatchEnd)).
      and(QueryBuilder.eq(syncTable.columns.state.name, State.RUNNING.toString()))).map {_.all()}
    
    if(slots.isSuccess) {
      if (slots.get.size() > 1) {
        logger.error(s"Inconsistant state. More than one version of job ${job.name} are running")
        None
      } else {
        if(slots.get.size() == 0) {
          None
        } else {
          Some(slots.get.get(0).getInt(syncTable.columns.slot.name))
        }
      }
    } else {
      None
    }
  }

  /**
   * Check if tables exists and tables are locked
   */
  private def crateTablesIfNotExists[T <: AbstractRow](job: JobInfo, dataColumns: T): Try[Unit] = Try {
    createKeyspaceAndTable(jobLockTable)
    createKeyspaceAndTable(syncTable)
    
    syncTable.columns.indexes match {
      case _: Some[List[String]] => createIndexStrings(syncTable).map { session.execute(_) }
      case _                     =>
    }
    
    // Register new job in lock table
    session.execute(QueryBuilder.insertInto(jobLockTable.keySpace, jobLockTable.tableName)
      .value(jobLockTable.columns.jobname.name, job.name)
      .value(jobLockTable.columns.locked.name, false))

    // Create data tables and register them in sync table
    0 to job.numberOfBatchSlots - 1 foreach { i =>

      // Create batch data tables
      session.execute(createSingleTableString(VoidTable(syncTable.keySpace, getBatchJobName(job.name, i), dataColumns)))

      // Register batch table
      session.execute(QueryBuilder.insertInto(syncTable.keySpace, syncTable.tableName)
        .value(syncTable.columns.slot.name, i)
        .value(syncTable.columns.online.name, false)
        .value(syncTable.columns.jobname.name, job.name)
        .value(syncTable.columns.locked.name, false)
        .value(syncTable.columns.state.name, State.NEW.toString())
        .value(syncTable.columns.latestUpdate.name, System.currentTimeMillis())
        .value(syncTable.columns.versions.name, job.numberOfBatchSlots)
        .value(syncTable.columns.batchStartTime.name, job.batchID.getBatchStart)
        .value(syncTable.columns.batchEndTime.name, job.batchID.getBatchEnd)
        .value(syncTable.columns.tableidentifier.name, getBatchJobName(syncTable.keySpace + "." + job.name, i))
        .ifNotExists)
    }

    // Create and register online tables
    0 to job.numberOfOnlineSlots - 1 foreach { i =>
      // Create online data tables
      // Columns[Column[_]]
      session.execute(createSingleTableString(VoidTable(syncTable.keySpace, getOnlineJobName(job.name, i), dataColumns)))

      // Register online tables
      session.execute(QueryBuilder.insertInto(syncTable.keySpace, syncTable.tableName)
        .value(syncTable.columns.online.name, true)
        .value(syncTable.columns.jobname.name, job.name)
        .value(syncTable.columns.slot.name, i)
        .value(syncTable.columns.versions.name, job.numberOfOnlineSlots)
        .value(syncTable.columns.locked.name, false)
        .value(syncTable.columns.state.name, State.NEW.toString())
        .value(syncTable.columns.latestUpdate.name, System.currentTimeMillis())
        .value(syncTable.columns.batchStartTime.name, job.batchID.getBatchStart)
        .value(syncTable.columns.batchEndTime.name, -1L)
        .value(syncTable.columns.tableidentifier.name, getOnlineJobName(syncTable.keySpace + "." + job.name, i))
        .ifNotExists)
    }
  }

  def getOnlineJobState(job: JobInfo, slot: Int): Option[State] = {
    this.getJobState(job, slot, true)
  }

  def getBatchJobState(job: JobInfo, version: Int): Option[State] = {
    this.getJobState(job, version, false)
  }
  
  private def getJobState(job: JobInfo, slot: Int, online: Boolean) : Option[State] = {
    execute(QueryBuilder.select(syncTable.columns.state.name).from(syncTable.keySpace, syncTable.tableName).where(
      QueryBuilder.eq(syncTable.columns.jobname.name, job.name)).
      and(QueryBuilder.eq(syncTable.columns.online.name, online)).
      and(QueryBuilder.eq(syncTable.columns.slot.name, slot)))
      .map { resultset => resultset.all().get(0).getString(0) }
      .map { state => State.values.find(_.toString() == state).get }
      .toOption
  }

  def getNewestOnlineSlot(job: JobInfo): Option[Int] = getNewestSlotAndTable(job.name, true).map(_._1)
  def getNewestBatchSlot(job: JobInfo): Option[Int] = getNewestSlotAndTable(job.name, false).map(_._1)

  private def getNewestSlotAndTable(jobName: String, online: Boolean): Option[(Int, String)] = {
    val headBatchQuery: RegularStatement = QueryBuilder.select().from(syncTable.keySpace, syncTable.tableName).
      where(QueryBuilder.eq(syncTable.columns.jobname.name, jobName))
      .and(QueryBuilder.eq(syncTable.columns.online.name, online))
      .and(QueryBuilder.eq(syncTable.columns.state.name, State.COMPLETED.toString()))

    // Find newest slot
    this.execute(headBatchQuery)
      .map { _.iterator() }.recover {
        case e => {logger.error(s"DB error while fetching Newest Version ${e.getMessage}"); throw e}
      }.toOption
      .flatMap { iter => this.getNewestRow(iter, syncTable.columns.latestUpdate.name)}
      .map { row => (row.getInt(syncTable.columns.slot.name), row.getString(syncTable.columns.tableidentifier.name)) } 
  }


  
  def insertInOnlineTable(job: JobInfo, slot: Int, data: RowWithValue): Try[Unit] = {
    val statement = data.foldLeft(QueryBuilder.insertInto(syncTable.keySpace, getOnlineJobName(job.name, slot))) {
      (acc, column) => acc.value(column.name, column.value)
    }

    logger.debug(s"Insert data in online table ${job.name} slot ${slot}: ${statement}")
    session.insert(statement) match {
      case Success(_) => Try()
      case Failure(message) => Failure(this.throwInsertStatementError(job.name, slot, statement, message.toString()))
    }
  }

  private def throwInsertStatementError(jobname: String, slot: Int, statement: Statement, message: String) = {
    throw new StatementExecutionError(s"Error while inserting data for job ${jobname}, slot ${slot}. Statement: ${statement}. ${message}")
  }
  
  def insertInBatchTable(job: JobInfo, slot: Int, data: RowWithValue): Try[Unit] = {
    val statement = data.foldLeft(QueryBuilder.insertInto(syncTable.keySpace, getBatchJobName(job.name, slot))) {
      (acc, column) => acc.value(column.name, column.value)
    }

    session.insert(statement) match {
      case Success(_) => Try()
      case Failure(message) => Failure(this.throwInsertStatementError(job.name, slot, statement, message.toString()))
    }
  }

  /**
   * Create base keyspace and table
   */
  def createKeyspaceAndTable[T <: AbstractRow](table: Table[T]): Try[Unit] = Try {
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS ${table.keySpace} WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1};")
    session.execute(createSingleTableString(table))
  }


  /**
   * Lock online table if it is used by another spark job.
   */
  def lockBatchTable(job: JobInfo): Try[Unit] = {
    logger.debug(s"Lock batch table for job: ${job.name}")

    val rowsToLock = new BatchStatement()
    0 to job.numberOfBatchSlots - 1 foreach {
      slot => rowsToLock.add(geLockStatement(job, slot, false, true))
    }
    executeQuorum(rowsToLock)
  }

  /**
   * Unlock batch table to make it available for a new job.
   */
  def unlockBatchTable(job: JobInfo): Try[Unit] = {
    logger.debug(s"Unlock batch table for job: ${job.name} ")

    val rowsToUnlock = new BatchStatement()
    0 to job.numberOfBatchSlots - 1 foreach {
      slot => rowsToUnlock.add(geLockStatement(job, slot, false, false))
    }
    executeQuorum(rowsToUnlock)
  }

    /**
   * Lock table if it is used by another spark job.
   */
  def lockOnlineTable(job: JobInfo): Try[Unit] = {
    logger.debug(s"Lock online table for job: ${job.name} ")

    val rowsToLock = new BatchStatement()
    0 to job.numberOfOnlineSlots - 1 foreach {
      slot => rowsToLock.add(geLockStatement(job, slot, true, true))
    }
    executeQuorum(rowsToLock)
  }
  
  /**
   * Unlock batch table to make it available for a new job.
   */
  def unlockOnlineTable(job: JobInfo): Try[Unit] = {
    logger.debug(s"Unlock online table for job: ${job.name}")

    val rowsToUnlock = new BatchStatement()
    0 to job.numberOfOnlineSlots - 1 foreach {
      slot => rowsToUnlock.add(geLockStatement(job, slot, true, false))
    }
    executeQuorum(rowsToUnlock)
  }

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
      val res = execute(QueryBuilder.select.all().from(syncTable.keySpace, syncTable.tableName).allowFiltering().where(
        QueryBuilder.eq(syncTable.columns.jobname.name, job.name)).
        and(QueryBuilder.eq(syncTable.columns.online.name, online)).
        and(QueryBuilder.eq(syncTable.columns.batchStartTime.name, job.batchID.getBatchStart)).
        and(QueryBuilder.eq(syncTable.columns.batchEndTime.name, job.batchID.getBatchEnd)).
        and(QueryBuilder.eq(syncTable.columns.locked.name, true)))
        res.map { rows => rows.all().size() > 0 }.toOption
    }

  def geLockStatement(job: JobInfo, slot: Int, online: Boolean, newState: Boolean): Conditions = {
    QueryBuilder.update(syncTable.keySpace, syncTable.tableName).
      `with`(QueryBuilder.set(syncTable.columns.locked.name, newState)).
      where(QueryBuilder.eq(syncTable.columns.jobname.name, job.name)).
      and(QueryBuilder.eq(syncTable.columns.batchStartTime.name, job.batchID.getBatchStart)).
      and(QueryBuilder.eq(syncTable.columns.batchEndTime.name, job.batchID.getBatchEnd)).
      and(QueryBuilder.eq(syncTable.columns.online.name, online)).
      and(QueryBuilder.eq(syncTable.columns.slot.name, slot)).
      onlyIf(QueryBuilder.eq(syncTable.columns.locked.name, !newState))
  }

  //  def getSynctable(jobName: String): Option[Table[Columns[ColumnV[_]]]] = {
  //    val rows = execute(QueryBuilder.select().all().from(syncTable.keySpace, syncTable.tableName).where(QueryBuilder.eq(syncTable.columns.jobname.name, jobName)))
  //    None
  //  }

  def getBatchJobData[T <: RowWithValue](jobname: String, slot: Int, result: T): Option[List[RowWithValue]] = {
    getJobData(jobname, slot, false, result)
  }
  def getOnlineJobData[T <: RowWithValue](jobname: String, slot: Int, result: T): Option[List[RowWithValue]] = {
    getJobData(jobname, slot, true, result)
  }

  private def getJobData[T <: RowWithValue](jobname: String, slot: Int, online: Boolean, result: T): Option[List[RowWithValue]] = {
    def handleColumnWithValue[U](currentRow: Row, destinationColumn: ColumnWithValue[U]): U = {
      val dbimpl = destinationColumn.dbimpl
      dbimpl.fromDBType(currentRow.get(destinationColumn.name, dbimpl.toDBType(destinationColumn.value).getClass()))
    }

    def fillValue[U](currentRow: Row, destinationColumn: ColumnWithValue[U]) = {
      destinationColumn.value = handleColumnWithValue(currentRow, destinationColumn)
    }

    val statementResult = online match {
      case true => execute(QueryBuilder.select().all().from(syncTable.keySpace, getOnlineJobName(jobname, slot))).map { x => x.iterator() }
      case _    => execute(QueryBuilder.select().all().from(syncTable.keySpace, getBatchJobName(jobname, slot))).map { x => x.iterator() }
    }
    var columns = new ListBuffer[RowWithValue]()

    if (statementResult.isFailure) {
      logger.error(s"No data for job ${jobname} ${slot} found")
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
          Failure(new StatementExecutionError(ex.getLocalizedMessage))
        } 
      }
  }

  private def execute(statement: RegularStatement): Try[ResultSet] = {
    logger.debug("Execute: " + statement)
    statement.setConsistencyLevel(ConsistencyLevel.QUORUM)

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

  def getNewestRow(rows: java.util.Iterator[Row], columnName: String): Option[Row] = {
    import scala.math.Ordering._
    val comp = implicitly[Ordering[Long]]
    getComptRow(rows, comp.gt, columnName)
  }

  def getOldestRow(rows: java.util.Iterator[Row], columnName: String): Option[Row] = {
    import scala.math.Ordering._
    val comp = implicitly[Ordering[Long]]
    getComptRow(rows, comp.lt, columnName)
  }

  def getComptRow(rows: JIterator[Row], comp: (Long, Long) => Boolean, columnName: String): Option[Row] = {
    @tailrec def accNewestRow(prevRow: Row, nextRows: JIterator[Row]): Row = {
      if (nextRows.hasNext) {
        val localRow = nextRows.next()
        logger.debug(s"Work with row ${localRow}")
        val max = if (comp(prevRow.getLong(columnName), localRow.getLong(columnName))) {
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

    if(rows.hasNext()) {
      Some(accNewestRow(rows.next(), rows))
    } else {
      None
    }
  }
}