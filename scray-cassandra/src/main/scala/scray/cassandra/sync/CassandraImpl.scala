package scray.cassandra.sync

import com.websudos.phantom.CassandraPrimitive
import scray.querying.sync.DBColumnImplementation
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
import java.util.ArrayList
import scray.querying.sync.DbSession
import scala.collection.mutable.ArrayBuffer
import scray.querying.sync.OnlineBatchSync
import scray.querying.sync.SyncTableBasicClasses.SyncTableRowEmpty
import scala.collection.mutable.ListBuffer
import scray.querying.sync.JobInfo
import com.datastax.driver.core.BatchStatement
import scray.querying.sync.State.State
import com.datastax.driver.core.querybuilder.Update.Where
import com.datastax.driver.core.querybuilder.Update.Conditions
import scray.querying.sync.SyncTable
import scala.util.Try
import scray.querying.sync.RunningJobExistsException
import scray.querying.sync.NoRunningJobExistsException
import scray.querying.sync.StatementExecutionError
import scala.util.Failure
import scala.util.Success
import scray.querying.description.TableIdentifier
import scala.collection.mutable.HashSet
import scray.querying.sync.AbstractRow
import scray.querying.sync.ColumnWithValue
import scray.querying.sync.VoidTable
import scray.querying.sync.RowWithValue
import scray.querying.sync.Table
import scray.querying.sync.State
import scray.querying.sync.AbstractTypeDetection
import scray.querying.sync.DBTypeImplicit
import com.datastax.driver.core.BatchStatement
import com.datastax.driver.core.Cluster
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.RegularStatement
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Row
import com.datastax.driver.core.Statement
import com.datastax.driver.core.querybuilder.Insert
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.typesafe.scalalogging.slf4j.LazyLogging
import com.websudos.phantom.CassandraPrimitive
import java.util.{ Iterator => JIterator }
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.HashSet
import scala.collection.mutable.ListBuffer
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scray.common.serialization.BatchID
import scray.querying.description.TableIdentifier
import scray.querying.sync.JobInfo
import scray.querying.sync.NoRunningJobExistsException
import scray.querying.sync.OnlineBatchSync
import scray.querying.sync.OnlineBatchSyncWithTableIdentifier
import scray.querying.sync.RunningJobExistsException
import scray.querying.sync.StateMonitoringApi
import scray.querying.sync.StatementExecutionError
import java.util.{ Iterator => JIterator }
import scray.querying.sync.JobLockTable
import scray.querying.sync.ArbitrarylyTypedRows
import scray.querying.sync.SyncTableBasicClasses
import scray.cassandra.util.CassandraUtils
import scray.querying.sync.Columns


object CassandraImplementation extends AbstractTypeDetection with Serializable {

  implicit def genericCassandraColumnImplicit[T](implicit cassImplicit: CassandraPrimitive[T]): DBColumnImplementation[T] = new DBColumnImplementation[T] {
    type RowType = Row
    override val rowConv = new DBRowImplementation[RowType] {
      override def convertRow(name: String, row: RowType): Option[T] = cassImplicit.fromRow(row, name)
    }
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

  def dbType[T: DBTypeImplicit]: DBColumnImplementation[T] = {
    val imp = implicitly[DBTypeImplicit[T]].asInstanceOf[CassandraPrimitive[T]]
    this.genericCassandraColumnImplicit(imp)
  }

  def strType: DBColumnImplementation[String] = genericCassandraColumnImplicit[String]
  def intType: DBColumnImplementation[Int] = genericCassandraColumnImplicit[Int]
  def lngType: DBColumnImplementation[Long] = genericCassandraColumnImplicit[Long]
  def boolType: DBColumnImplementation[Boolean] = genericCassandraColumnImplicit[Boolean]

}

class OnlineBatchSyncCassandra(dbSession: DbSession[Statement, Insert, ResultSet]) extends OnlineBatchSync[Statement, Insert, ResultSet] with OnlineBatchSyncWithTableIdentifier[Statement, Insert, ResultSet] with StateMonitoringApi[Statement, Insert, ResultSet] {

  import CassandraImplementation.genericCassandraColumnImplicit

  def this(dbHostname: String) = {
    this(new CassandraDbSession(Cluster.builder().addContactPoint(dbHostname).build().connect()))
  }

  def this(dbHostname: String, port: Int) = {
    this(new CassandraDbSession(Cluster.builder().addContactPoint(dbHostname).withPort(port).build().connect()))
  }

  // Create or use a given DB session.
  @transient val session = dbSession

  val syncTable = SyncTable("silidx", "SyncTable")
  val jobLockTable = JobLockTable("silidx", "JobLockTable")
  val statementGenerator = CassandraUtils
  val lockTimeOut = 500 //ms

  /**
   * Create and register tables for a new job.
   */
  @Override
  def initJob[T <: AbstractRow](job: JOB_INFO, dataTable: T): Try[Unit] = Try {
    statementGenerator.createKeyspaceCreationStatement(syncTable).map { statement => dbSession.execute(statement) }
    statementGenerator.createTableStatement(syncTable).map { statement => dbSession.execute(statement) }

    this.crateAndRegisterTablesIfNotExists(job)
    this.createDataTables(job, dataTable)
  }

  @Override
  def initJob[DataTableT <: ArbitrarylyTypedRows](job: JOB_INFO): Try[Unit] = {
    statementGenerator.createKeyspaceCreationStatement(syncTable).map { statement => dbSession.execute(statement) }
    statementGenerator.createTableStatement(syncTable).map { statement => dbSession.execute(statement) }

    this.crateAndRegisterTablesIfNotExists(job)
  }

  def startInicialBatch(job: JOB_INFO, batchID: BatchID): Try[Unit] = {
    def createInicialBatchStatement(slot: Int, online: Boolean, startTime: Long, endTime: Long): Statement = {
      QueryBuilder.update(syncTable.keySpace, syncTable.tableName)
        .`with`(QueryBuilder.set(syncTable.columns.state.name, State.RUNNING.toString()))
        .and(QueryBuilder.set(syncTable.columns.batchStartTime.name, startTime))
        .and(QueryBuilder.set(syncTable.columns.batchEndTime.name, endTime))
        .where(QueryBuilder.eq(syncTable.columns.jobname.name, job.name))
        .and(QueryBuilder.eq(syncTable.columns.online.name, online))
        .and(QueryBuilder.eq(syncTable.columns.slot.name, slot))
    }

    this.executeQuorum((createInicialBatchStatement(0, false, batchID.getBatchStart, batchID.getBatchEnd)))
  }

  @Override
  def startNextBatchJob(job: JOB_INFO): Try[Unit] = {
    logger.debug(s"Start next batch job ${job.name}")
    job.getLock(session).transaction(this.startJob, job, false)
  }

  @Override
  def startNextOnlineJob(job: JOB_INFO): Try[Unit] = {
    logger.debug(s"Start next batch job ${job.name}")
    job.getLock(dbSession).transaction(this.startJob, job, true)
  }

  def startNextBatchJob(job: JOB_INFO, dataTable: TableIdentifier): Try[Unit] = ???

  private def startJob(job: JOB_INFO, online: Boolean): Try[Unit] = {
    def createStartStatement(slot: Int, online: Boolean, startTime: Long): Statement = {
      QueryBuilder.update(syncTable.keySpace, syncTable.tableName)
        .`with`(QueryBuilder.set(syncTable.columns.state.name, State.RUNNING.toString()))
        .and(QueryBuilder.set(syncTable.columns.batchStartTime.name, startTime))
        .where(QueryBuilder.eq(syncTable.columns.jobname.name, job.name))
        .and(QueryBuilder.eq(syncTable.columns.online.name, online))
        .and(QueryBuilder.eq(syncTable.columns.slot.name, slot))
    }

    if (online) {
      // Abort if a job is already running
      this.getRunningOnlineJobSlot(job) match {
        case Some(slot) => {
          logger.error(s"Job ${job.name} is currently running on slot ${slot}")
          Failure(new RunningJobExistsException(s"Online job ${job.name} slot ${slot} is currently running"))
        }
        case None => {
          val slot = (getNewestOnlineSlot(job).getOrElse({ logger.debug("No completed slot found use 0"); 0 }) + 1) % job.numberOfOnlineSlots
          logger.debug(s"Set next online slot to ${slot}")
          executeQuorum(createStartStatement(slot, true, job.startTime.getOrElse(System.currentTimeMillis())))
        }
      }
    } else {
      // Abort if a job is already running
      this.getRunningBatchJobSlot(job) match {
        case Some(slot) => {
          logger.error(s"Job ${job.name} is currently running on slot ${slot}")
          Failure(new RunningJobExistsException(s"Batch job ${job.name} with slot ${slot} is currently running"))
        }
        case None => {
          val newSlot = (getNewestBatchSlot(job).getOrElse({ logger.debug("No completed slot found use slot 1"); 0 }) + 1) % job.numberOfBatchSlots
          val startTime = job.startTime match {
            case Some(startTime) => startTime
            case None => getBatchID(job) match {
              case Some(id) => id.getBatchEnd
              case None     => System.currentTimeMillis()
            }
          }

          logger.debug(s"Set next batch slot to ${newSlot}")
          executeQuorum(createStartStatement(newSlot, false, startTime))
        }
      }
    }
  }

  def completeBatchJob(job: JOB_INFO): Try[Unit] = Try {
    logger.debug(s"Complete batch job ${job}")

    getRunningBatchJobSlot(job) match {
      case slot: Some[Int] =>
        this.completeJob(job, slot.get, false)
      case None => throw new NoRunningJobExistsException(s"No running job with name: ${job.name} exists.")
    }
  }

  def completeOnlineJob(job: JOB_INFO): Try[Unit] = Try {
    getRunningOnlineJobSlot(job) match {
      case slot: Some[Int] =>
        this.completeJob(job, slot.get, true)
      case None => throw new NoRunningJobExistsException(s"No running job with name: ${job.name} exists.")
    }
  }

  def resetBatchJob(job: JOB_INFO): Try[Unit] = Try {
    val runningBatchSlot = this.getRunningBatchJobSlot(job).getOrElse(0)
    this.renewJob(job, runningBatchSlot, false)
  }

  def resetOnlineJob(job: JOB_INFO): Try[Unit] = Try {
    val runningOnlineSlot = this.getRunningOnlineJobSlot(job).getOrElse(0)
    this.renewJob(job, runningOnlineSlot, true)
  }

  /**
   * Set running job to state new
   */
  private def renewJob(job: JOB_INFO, slot: Int, online: Boolean): Try[Unit] = {
    val query = QueryBuilder.update(syncTable.keySpace, syncTable.tableName)
      .`with`(QueryBuilder.set(syncTable.columns.state.name, State.NEW.toString()))
      .where(QueryBuilder.eq(syncTable.columns.jobname.name, job.name))
      .and(QueryBuilder.eq(syncTable.columns.online.name, online))
      .and(QueryBuilder.eq(syncTable.columns.slot.name, slot))
    executeQuorum(query)
  }

  private def completeJob(job: JOB_INFO, slot: Int, online: Boolean): Try[Unit] = {
    val query = QueryBuilder.update(syncTable.keySpace, syncTable.tableName)
      .`with`(QueryBuilder.set(syncTable.columns.state.name, State.COMPLETED.toString()))
      .and(QueryBuilder.set(syncTable.columns.batchEndTime.name, job.endTime.getOrElse(System.currentTimeMillis())))
      .where(QueryBuilder.eq(syncTable.columns.jobname.name, job.name))
      .and(QueryBuilder.eq(syncTable.columns.online.name, online))
      .and(QueryBuilder.eq(syncTable.columns.slot.name, slot))
    executeQuorum(query)
  }

  override def getQueryableTableIdentifiers: List[(String, TableIdentifier, Int)] = {

    val jobnames = new HashSet[String]()
    //    val query = QueryBuilder.select(syncTable.columns.jobname.name).from(syncTable.keySpace, syncTable.tableName)
    //    val results = execute(query)
    //    results.map { resultset =>
    //      resultset.iterator.asScala.map { row =>
    //        val currJob = row.getString(syncTable.columns.jobname.name)
    //        jobnames(currJob) match {
    //          case true => None
    //          case false =>
    //            jobnames += currJob
    //            val (slot, tablename) = getNewestVersion(currJob, true).getOrElse {
    //              getNewestVersion(currJob, false).getOrElse {
    //                (-1, "")
    //              }
    //            }
    //            if (slot > -1) {
    //              Some((currJob, splitIntoTableIdentifier(tablename), slot))
    //            } else {
    //              None
    //            }
    //        }
    //        // row.getString(syncTable.columns.tableidentifier.name).split("\\.")
    //      }.filter(_.isDefined).map(_.get).toList
    //    }.getOrElse(List())

    List()
  }

  def getRunningBatchJobSlot(job: JOB_INFO): Option[Int] = {
    this.getRunningSlot(job, false)
  }

  def getTableIdentifierOfRunningJob(job: JobInfo[Statement, Insert, ResultSet]): Option[TableIdentifier] = {
    val tableIdentifierSelectStatement = QueryBuilder.select(syncTable.columns.dbId.name, syncTable.columns.tableId.name).from(syncTable.keySpace, syncTable.tableName).where(
      QueryBuilder.eq(syncTable.columns.jobname.name, job.name)).
      and(QueryBuilder.eq(syncTable.columns.online.name, false)).
      and(QueryBuilder.eq(syncTable.columns.state.name, State.RUNNING.toString()))

    this.execute(tableIdentifierSelectStatement)
      .map { _.iterator() }.recover {
        case e => { logger.error(s"DB error while fetching TableIdentifier ${e.getMessage}"); throw e }
      }.toOption.flatMap { row =>
        val currentRow = row.next()
        Some(new TableIdentifier(currentRow.getString(syncTable.columns.dbSystem.name), currentRow.getString(syncTable.columns.dbId.name), currentRow.getString(syncTable.columns.tableId.name)))
      }
  }

  def getLatestBatch(job: JOB_INFO): Option[Int] = {
    val slotQuery = QueryBuilder.select.all().from(syncTable.keySpace, syncTable.tableName).where(
      QueryBuilder.eq(syncTable.columns.jobname.name, job.name)).
      and(QueryBuilder.eq(syncTable.columns.online.name, false)).
      and(QueryBuilder.eq(syncTable.columns.state.name, State.COMPLETED.toString()))

    this.execute(slotQuery)
      .map { _.iterator() }.recover {
        case e => { logger.error(s"DB error while fetching newest slot ${e.getMessage}"); throw e }
      }.toOption
      .flatMap { iter => CassandraUtils.getNewestRow(iter, syncTable.columns.batchEndTime.name) }
      .map { row => (row.getInt(syncTable.columns.slot.name)) }
  }

  def getRunningOnlineJobSlot(job: JOB_INFO): Option[Int] = {
    this.getRunningSlot(job, true)
  }

  private def getRunningSlot(job: JOB_INFO, online: Boolean): Option[Int] = {

    val slots = execute(QueryBuilder.select().all().from(syncTable.keySpace, syncTable.tableName).allowFiltering().where(
      QueryBuilder.eq(syncTable.columns.jobname.name, job.name)).
      and(QueryBuilder.eq(syncTable.columns.online.name, online)).
      and(QueryBuilder.eq(syncTable.columns.state.name, State.RUNNING.toString()))).map { _.all() }

    if (slots.isSuccess) {
      if (slots.get.size() > 1) {
        logger.error(s"Inconsistant state. More than one version of job ${job.name} are running")
        None
      } else {
        if (slots.get.size() == 0) {
          None
        } else {
          Some(slots.get.get(0).getInt(syncTable.columns.slot.name))
        }
      }
    } else {
      None
    }
  }

  def getBatchID(job: JOB_INFO): Option[BatchID] = {
    val slotQuery = QueryBuilder.select.all().from(syncTable.keySpace, syncTable.tableName).where(
      QueryBuilder.eq(syncTable.columns.jobname.name, job.name)).
      and(QueryBuilder.eq(syncTable.columns.online.name, false)).
      and(QueryBuilder.eq(syncTable.columns.state.name, State.COMPLETED.toString()))

    val start = this.execute(slotQuery)
      .map { _.iterator() }.recover {
        case e => { logger.error(s"DB error while fetching newest slot ${e.getMessage}"); throw e }
      }.toOption
      .flatMap { iter =>
        {
          val comp = implicitly[Ordering[Long]]
          this.getComptRow(iter, comp.lt, syncTable.columns.batchEndTime.name)
        }
      }.map { row => (row.getLong(syncTable.columns.batchStartTime.name)) }

    val end = this.execute(slotQuery)
      .map { _.iterator() }.recover {
        case e => { logger.error(s"DB error while fetching newest slot ${e.getMessage}"); throw e }
      }.toOption
      .flatMap { iter =>
        {
          val comp = implicitly[Ordering[Long]]
          this.getComptRow(iter, comp.lt, syncTable.columns.batchEndTime.name)
        }
      }.map { row => (row.getLong(syncTable.columns.batchEndTime.name)) }

    start match {
      case Some(value) => Some(new BatchID(start.get, end.get))
      case None        => None
    }
  }

  private def crateAndRegisterTablesIfNotExists[T <: AbstractRow](job: JOB_INFO): Try[Unit] = Try {

    syncTable.columns.indexes match {
      case _: Some[List[String]] => createIndexStrings(syncTable).map { session.execute(_) }
      case _                     =>
    }

    if (!checkIfJobExists(job).getOrElse(false)) {
      // Register online and batch tables
      val statements = new BatchStatement()
      0 to job.numberOfBatchSlots - 1 foreach { i =>
        // Register batch tables
        statements.add(QueryBuilder.insertInto(syncTable.keySpace, syncTable.tableName)
          .value(syncTable.columns.slot.name, i)
          .value(syncTable.columns.online.name, false)
          .value(syncTable.columns.jobname.name, job.name)
          .value(syncTable.columns.state.name, State.NEW.toString())
          .value(syncTable.columns.versions.name, job.numberOfBatchSlots)
          .value(syncTable.columns.dbSystem.name, "cassandra")
          .value(syncTable.columns.dbId.name, syncTable.keySpace)
          .value(syncTable.columns.tableId.name, getBatchJobName(job.name, i)))
      }

      // Create and register online tables
      0 to job.numberOfOnlineSlots - 1 foreach { i =>
        // Register online tables
        statements.add(QueryBuilder.insertInto(syncTable.keySpace, syncTable.tableName)
          .value(syncTable.columns.slot.name, i)
          .value(syncTable.columns.online.name, true)
          .value(syncTable.columns.jobname.name, job.name)
          .value(syncTable.columns.state.name, State.NEW.toString())
          .value(syncTable.columns.versions.name, job.numberOfBatchSlots)
          .value(syncTable.columns.dbSystem.name, "cassandra")
          .value(syncTable.columns.dbId.name, syncTable.keySpace)
          .value(syncTable.columns.tableId.name, getOnlineJobName(job.name, i)))
      }
      job.getLock(dbSession).transaction(this.executeQuorum, statements)
    }
  }

  def checkIfJobExists(job: JOB_INFO): Option[Boolean] = {

    // Check if tables already exists.
    val existsStatement = QueryBuilder.select(syncTable.columns.jobname.name)
      .from(syncTable.keySpace, syncTable.tableName)
      .where(QueryBuilder.eq(syncTable.columns.jobname.name, job.name))
      .and(QueryBuilder.eq(syncTable.columns.online.name, false))
      .and(QueryBuilder.eq(syncTable.columns.slot.name, 0))

    job.getLock(dbSession).transaction(this.execute, existsStatement).map { _.iterator().hasNext() } match {
      case Success(exists) => Some(exists)
      case Failure(error)  => { logger.error(s"Unable to check if job ${job.name} exits."); None }
    }
  }

  def createDataTables[T <: AbstractRow](job: JOB_INFO, dataColumns: T): Try[Unit] = Try {
    // Create and register online tables
    0 to job.numberOfOnlineSlots - 1 foreach { i =>
      // Create online data tables
      statementGenerator.createTableStatement(VoidTable(syncTable.keySpace, getOnlineJobName(job.name, i), dataColumns)).map { statement => session.execute(statement) }
    }

    0 to job.numberOfBatchSlots - 1 foreach { i =>
      // Create batch data tables
      statementGenerator.createTableStatement(VoidTable(syncTable.keySpace, getBatchJobName(job.name, i), dataColumns)).map { statement => session.execute(statement) }
    }
  }

  def getOnlineJobState(job: JOB_INFO, slot: Int): Option[State] = {
    this.getJobState(job, slot, true)
  }

  def getBatchJobState(job: JOB_INFO, slot: Int): Option[State] = {
    this.getJobState(job, slot, false)
  }

  private def getJobState(job: JOB_INFO, slot: Integer, online: Boolean): Option[State] = {
    execute(QueryBuilder.select(syncTable.columns.state.name).from(syncTable.keySpace, syncTable.tableName).allowFiltering().where(
      QueryBuilder.eq(syncTable.columns.jobname.name, job.name)).
      and(QueryBuilder.eq(syncTable.columns.slot.name, slot))
      and (QueryBuilder.eq(syncTable.columns.online.name, online)))
      .map { resultset => resultset.all().get(0).getString(0) }
      .map { state => State.values.find(_.toString() == state).get }
      .toOption
  }

  /**
   * Get latest version for a given batch job.
   */
  def getBatchVersion(job: JOB_INFO): Option[TableIdentifier] = {
    getNewestVersion(job.name, false).flatMap(row => Some(new TableIdentifier(row.dbSystem.value, row.dbId.value, row.tableId.value)))
  }

  /**
   * Get latest version for a given online job.
   */
  def getOnlineVersion(job: JOB_INFO): Option[TableIdentifier] = {
    getNewestVersion(job.name, true).flatMap(row => Some(new TableIdentifier(row.dbSystem.value, row.dbId.value, row.tableId.value)))
  }

  def getNewestOnlineSlot(job: JOB_INFO): Option[Int] = getNewestVersion(job.name, true).map { _.slot.value }
  def getNewestBatchSlot(job: JOB_INFO): Option[Int] = getNewestVersion(job.name, false).map(_.slot.value)

  def getNewestVersion(jobName: String, online: Boolean): Option[scray.querying.sync.SyncTableBasicClasses.SyncTableRow] = {
    val headBatchQuery: RegularStatement = QueryBuilder.select().from(syncTable.keySpace, syncTable.tableName).
      where(QueryBuilder.eq(syncTable.columns.jobname.name, jobName))
      .and(QueryBuilder.eq(syncTable.columns.online.name, online))
      .and(QueryBuilder.eq(syncTable.columns.state.name, State.COMPLETED.toString()))

    // Find newest slot
    this.execute(headBatchQuery)
      .map { _.iterator() }.recover {
        case e => { logger.error(s"DB error while fetching Newest Version ${e.getMessage}"); throw e }
      }.toOption
      .flatMap { iter => CassandraUtils.getNewestRow(iter, syncTable.columns.batchEndTime.name) }
      .map { row =>
        new scray.querying.sync.SyncTableBasicClasses.SyncTableRow(
          row.getString(syncTable.columns.jobname.name),
          row.getInt(syncTable.columns.slot.name),
          row.getInt(syncTable.columns.versions.name),
          row.getString(syncTable.columns.dbSystem.name),
          row.getString(syncTable.columns.dbId.name),
          row.getString(syncTable.columns.tableId.name),
          row.getLong(syncTable.columns.batchStartTime.name),
          row.getLong(syncTable.columns.batchEndTime.name),
          row.getBool(syncTable.columns.online.name),
          row.getString(syncTable.columns.state.name),
          row.getString(syncTable.columns.mergeMode.name),
          row.getLong(syncTable.columns.firstElementTime.name))
      }

  }

  def insertInOnlineTable(job: JOB_INFO, slot: Int, data: RowWithValue): Try[Unit] = {
    val statement = data.foldLeft(QueryBuilder.insertInto(syncTable.keySpace, getOnlineJobName(job.name, slot))) {
      (acc, column) => acc.value(column.name, column.value)
    }

    logger.debug(s"Insert data in online table ${job.name} slot ${slot}: ${statement}")
    session.insert(statement) match {
      case Success(_)       => Try()
      case Failure(message) => Failure(this.throwInsertStatementError(job.name, slot, statement, message.toString()))
    }
  }

  private def throwInsertStatementError(jobname: String, slot: Int, statement: Statement, message: String) = {
    throw new StatementExecutionError(s"Error while inserting data for job ${jobname}, slot ${slot}. Statement: ${statement}. ${message}")
  }

  def insertInBatchTable(job: JOB_INFO, slot: Int, data: RowWithValue): Try[Unit] = {
    val table = new Table(syncTable.keySpace, getBatchJobName(job.name, slot), data)
    CassandraUtils.createTableStatement(table).map { session.execute(_) }
    val statement = data.foldLeft(QueryBuilder.insertInto(syncTable.keySpace, getBatchJobName(job.name, slot))) {
      (acc, column) => acc.value(column.name, column.value)
    }

    session.insert(statement) match {
      case Success(_)       => Try()
      case Failure(message) => Failure(this.throwInsertStatementError(job.name, slot, statement, message.toString()))
    }
  }
  
  def insertInBatchTable(job: JOB_INFO, data: RowWithValue): Try[Unit] = {
    val insertResult =  this.getRunningBatchJobSlot(job).map { slot => 
      logger.debug(s"Insert data in batch table for job ${job}. Slot: ${slot}")
      this.insertInBatchTable(job: JOB_INFO, slot: Int, data: RowWithValue)    
    }
    
    insertResult match {
      case None => Failure(new NoRunningJobExistsException(s"Unable to insert data ${data} for job ${job}"))
      case Some(exception) => exception
    }
  }

  //  /**
  //   * Lock online table if it is used by another spark job.
  //   */
  //  def lockBatchTable(job: JOB_INFO): Try[Unit] = {
  //    logger.debug(s"Lock batch table for job: ${job.name}")
  //
  //    val rowsToLock = new BatchStatement()
  //    0 to job.numberOfBatchSlots - 1 foreach {
  //      slot => rowsToLock.add(geLockStatement(job, slot, false, true))
  //    }
  //     job.getLock(dbSession).transaction(this.executeQuorum, rowsToLock)
  //  }

  //  /**
  //   * Unlock batch table to make it available for a new job.
  //   */
  //  def unlockBatchTable(job: JOB_INFO): Try[Unit] = {
  //    logger.debug(s"Unlock batch table for job: ${job.name} ")
  //
  //    val rowsToUnlock = new BatchStatement()
  //    0 to job.numberOfBatchSlots - 1 foreach {
  //      slot => rowsToUnlock.add(geLockStatement(job, slot, false, false))
  //    }
  //     job.getLock(dbSession).transaction(executeQuorum, rowsToUnlock)
  //  }
  //
  //    /**
  //   * Lock table if it is used by another spark job.
  //   */
  //  def lockOnlineTable(job: JOB_INFO): Try[Unit] = {
  //    logger.debug(s"Lock online table for job: ${job.name} ")
  //      val rowsToLock = new BatchStatement()
  //      0 to job.numberOfOnlineSlots - 1 foreach {
  //        slot => rowsToLock.add(geLockStatement(job, slot, true, true))
  //      }
  //      job.getLock(dbSession).transaction(executeQuorum, rowsToLock)
  //  }
  //  
  //  /**
  //   * Unlock batch table to make it available for a new job.
  //   */
  //  def unlockOnlineTable(job: JOB_INFO): Try[Unit] = {
  //    logger.debug(s"Unlock online table for job: ${job.name}")
  //
  //      val rowsToUnlock = new BatchStatement()
  //      0 to job.numberOfOnlineSlots - 1 foreach {
  //        slot => rowsToUnlock.add(geLockStatement(job, slot, true, false))
  //      }
  //      job.getLock(dbSession).transaction(executeQuorum, rowsToUnlock)
  //  }

  private def isTableLocked(job: JOB_INFO, online: Boolean): Option[Boolean] = {
    val res = execute(QueryBuilder.select.all().from(syncTable.keySpace, syncTable.tableName).allowFiltering().where(
      QueryBuilder.eq(syncTable.columns.jobname.name, job.name)).
      and(QueryBuilder.eq(syncTable.columns.online.name, online)))
    res.map { rows => rows.all().size() > 0 }.toOption
  }

  //  def geLockStatement(job: JOB_INFO, slot: Int, online: Boolean, newState: Boolean): Conditions = {
  //    QueryBuilder.update(syncTable.keySpace, syncTable.tableName).
  //      `with`(QueryBuilder.set(syncTable.columns.locked.name, newState)).
  //      where(QueryBuilder.eq(syncTable.columns.jobname.name, job.name)).
  //      and(QueryBuilder.eq(syncTable.columns.batchStartTime.name, job.batchID.getBatchStart)).
  //      and(QueryBuilder.eq(syncTable.columns.batchEndTime.name, job.batchID.getBatchEnd)).
  //      and(QueryBuilder.eq(syncTable.columns.online.name, online)).
  //      and(QueryBuilder.eq(syncTable.columns.slot.name, slot)).
  //      onlyIf(QueryBuilder.eq(syncTable.columns.locked.name, !newState))
  //  }

  def getOnlineStartTime(job: JOB_INFO): Option[Long] = {
    val slots = execute(QueryBuilder.select().all().from(syncTable.keySpace, syncTable.tableName).allowFiltering().where(
      QueryBuilder.eq(syncTable.columns.jobname.name, job.name)).
      and(QueryBuilder.eq(syncTable.columns.online.name, true)).
      and(QueryBuilder.eq(syncTable.columns.state.name, State.RUNNING.toString()))).map { _.all() }

    if (slots.isSuccess) {
      if (slots.get.size() > 1) {
        logger.error(s"Inconsistant state. More than one version of job ${job.name} are running")
        None
      } else {
        if (slots.get.size() == 0) {
          None
        } else {
          Some(slots.get.get(0).getLong(syncTable.columns.firstElementTime.name))
        }
      }
    } else {
      None
    }
  }
  
  def setOnlineStartTime(job: JOB_INFO, time: Long): Try[Unit] = {
    this.getRunningOnlineJobSlot(job).map { slot => 
      val insertTime = QueryBuilder.insertInto(syncTable.keySpace, syncTable.tableName)
            .value(syncTable.columns.slot.name, slot)
            .value(syncTable.columns.online.name, true)
            .value(syncTable.columns.jobname.name, job.name)
            .value(syncTable.columns.state.name, State.RUNNING.toString())
            .value(syncTable.columns.versions.name, job.numberOfOnlineSlots)
            .value(syncTable.columns.dbSystem.name, "cassandra")
            .value(syncTable.columns.dbId.name, syncTable.keySpace)
            .value(syncTable.columns.firstElementTime.name, time)
            .value(syncTable.columns.tableId.name, getBatchJobName(job.name, slot))
            
            dbSession.execute(insertTime)
    } match {
      case Some(_) => Try()
      case None => Failure(new RuntimeException("Error while setting start time. See previous logs")) 
    }
  }
  
  def getBatchJobData[T <: RowWithValue, K](jobname: String, slot: Int, key: K, result: T): Option[RowWithValue] = {
    getJobData(jobname, slot, false, Some(key), result).map { row => row.head } // One key is referred to one key
  }
  
  def getBatchJobData[T <: RowWithValue](jobInfo: JOB_INFO, result: T): Option[List[RowWithValue]] = {
    val latestCompletedBatch = this.getLatestBatch(jobInfo).getOrElse(0)
    this.getBatchJobData(jobInfo.name, latestCompletedBatch, result)
  }

  def getOnlineJobData[T <: RowWithValue, K](jobname: String, slot: Int, key: K, result: T): Option[List[RowWithValue]] = {
    getJobData(jobname, slot, true, Some(key), result)
  }

  def getBatchJobData[T <: RowWithValue](jobname: String, slot: Int, result: T): Option[List[RowWithValue]] = {
    getJobData(jobname, slot, false, None, result)
  }
  def getOnlineJobData[T <: RowWithValue](jobname: String, slot: Int, result: T): Option[List[RowWithValue]] = {
    getJobData(jobname, slot, true, None, result)
  }

  private def getJobData[T <: RowWithValue, K](jobname: String, slot: Int, online: Boolean, key: Option[K], result: T): Option[List[RowWithValue]] = {
    def handleColumnWithValue[U](currentRow: Row, destinationColumn: ColumnWithValue[U]): U = {
      val dbimpl = destinationColumn.dbimpl
      dbimpl.fromDBRow(destinationColumn.name, currentRow).get
      dbimpl.fromDBType(currentRow.get(destinationColumn.name, dbimpl.toDBType(destinationColumn.value).getClass()))
    }

    def fillValue[U](currentRow: Row, destinationColumn: ColumnWithValue[U]) = {
      destinationColumn.value = handleColumnWithValue(currentRow, destinationColumn)
    }

    def getbatchResults(online: Boolean) = {
      online match {
        case true => execute(QueryBuilder.select().all().from(syncTable.keySpace, getOnlineJobName(jobname, slot))).map { x => x.iterator() }
        case _    => execute(QueryBuilder.select().all().from(syncTable.keySpace, getBatchJobName(jobname, slot))).map { x => x.iterator() }
      }
    }

    def getKeyBasedBatchResults[K](online: Boolean, key: K) = {
      online match {
        case true => execute(QueryBuilder.select().all().from(syncTable.keySpace, getOnlineJobName(jobname, slot))
          .where(QueryBuilder.eq(result.primaryKey.replace("(", "").replace(")", ""), key))).map { x => x.iterator() }
        case _ => execute(QueryBuilder.select().all().from(syncTable.keySpace, getBatchJobName(jobname, slot))
          .where(QueryBuilder.eq(result.primaryKey.replace("(", "").replace(")", ""), key))).map { x => x.iterator() }
      }
    }

    val statementResult = key match {
      case Some(key) => getKeyBasedBatchResults(online, key)
      case None      => getbatchResults(online)
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
          val casRow = dbDataIter.next()
          nextRow.columns.map { destinationColumn =>
            fillValue(casRow, destinationColumn)
          }
          columns += nextRow
        }
        Some(columns.toList)
      } else {
        None
      }
    }
    
    // Return result
    if(columns.isEmpty) {
      None
    } else {
      Some(columns.toList)
    }
  }

  private def executeQuorum(statement: Statement): Try[Unit] = {

    statement match {
      case bStatement: BatchStatement => logger.debug("Execute batch statement: " + bStatement.getStatements)
      case _                          => logger.debug("Execute query: " + statement)
    }

    statement.setConsistencyLevel(ConsistencyLevel.QUORUM)
    dbSession.execute(statement) match {
      case Success(result) => Try()
      case Failure(ex) => {
        logger.warn(s"Error while executing statement: ${statement}.}")
        logger.debug(s"Error while executing statement: ${statement}. ${ex.printStackTrace()}")
        Failure(new StatementExecutionError(ex.getLocalizedMessage))
      }
    }
  }

  private def execute(statement: RegularStatement): Try[ResultSet] = {
    logger.debug("Execute: " + statement)
    // statement.setConsistencyLevel(ConsistencyLevel.QUORUM)

    dbSession.execute(statement) match {
      case Success(result) => Try(result)
      case Failure(ex) => {
        logger.warn(s"Error while executing statement: ${statement}.")
        logger.debug(s"Error while executing statement: ${statement}. ${ex.printStackTrace()}")
        Try(throw new StatementExecutionError(ex.getLocalizedMessage))
      }
    }
  }

  private def createIndexStrings[T <: AbstractRow](table: Table[T]): List[String] = {

    def addString(column: String): String = {
      s"CREATE INDEX IF NOT EXISTS ON ${table.keySpace}.${table.tableName} (${column})"
    }

    table.columns.indexes.getOrElse(List("")).foldLeft(List[String]())((acc, indexStatement) => addString(indexStatement) :: acc)
  }

  private def getBatchJobName(jobname: String, nr: Int): String = { jobname + "_batch" + nr }
  private def getOnlineJobName(jobname: String, nr: Int): String = { jobname + "_online" + nr }

    def getComptRow(rows: JIterator[Row], comp: (Long, Long) => Boolean, columnName: String): Option[Row] = {
    @tailrec def accNewestRow(prevRow: Row, nextRows: JIterator[Row]): Row = {
      if (nextRows.hasNext) {
        val localRow = nextRows.next()
        logger.debug(s"Work with row ${prevRow} and ${localRow}")
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

    if (rows.hasNext()) {
      Some(accNewestRow(rows.next(), rows))
    } else {
      None
    }
  }
}
