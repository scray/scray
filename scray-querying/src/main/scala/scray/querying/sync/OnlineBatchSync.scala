package scray.querying.sync

import java.util.{ Iterator => JIterator }

import scala.annotation.tailrec
import scala.util.Try

import com.datastax.driver.core.Statement
import com.typesafe.scalalogging.slf4j.LazyLogging

import scray.common.serialization.BatchID
import scray.querying.description.TableIdentifier
import scray.querying.sync.types._
import scray.querying.sync.types._
import scray.querying.sync.types.ArbitrarylyTypedRows
import scray.querying.sync.types.RowWithValue
import scray.querying.sync.types.State
import scray.querying.sync.types.State.State
import scray.querying.sync.types.LockApi
import scalaz.Monoid


trait OnlineBatchSyncWithTableIdentifier[Statement, InsertIn, Result] extends LazyLogging {

  /**
   * Prepare tables for this job.
   */
  def initJob[DataTableT <: ArbitrarylyTypedRows](job: JobInfo[Statement, InsertIn, Result]): Try[Unit]

  /**
   * Start new batch job in next slot. If no job is running.
   */
  def startNextBatchJob(job: JobInfo[Statement, InsertIn, Result], dataTable: TableIdentifier): Try[Unit]
  
   /**
   * Start new batch job in next slot. If no job is running.
   */
  def startNextOnlineJob(job: JobInfo[Statement, InsertIn, Result]): Try[Unit]
  
  /**
   * Mark the given batch job as completed.
   */
  def completeBatchJob(job: JobInfo[Statement, InsertIn, Result]): Try[Unit]
  
   /**
   * Mark the given online job as completed.
   */
  def completeOnlineJob(job: JobInfo[Statement, InsertIn, Result]): Try[Unit]
    
  def getQueryableTableIdentifiers: List[(String, TableIdentifier, Int)]
  def getTableIdentifier(job: JobInfo[Statement, InsertIn, Result]): TableIdentifier 
}

trait OnlineBatchSync[Statement, InsertIn, Result] extends LazyLogging {

  type JOB_INFO = JobInfo[Statement, InsertIn, Result]
  
  def initJob[DataTableT <: ArbitrarylyTypedRows](job: JobInfo[Statement, InsertIn, Result], dataTable: DataTableT): Try[Unit]
  
  def startInicialBatch(job: JOB_INFO, batchID: BatchID): Try[Unit]
  
  def startNextBatchJob(job: JobInfo[Statement, InsertIn, Result]): Try[Unit]
  def startNextOnlineJob(job: JobInfo[Statement, InsertIn, Result]): Try[Unit]
  
  def completeBatchJob(job: JobInfo[Statement, InsertIn, Result]): Try[Unit]
  def completeOnlineJob(job: JobInfo[Statement, InsertIn, Result]): Try[Unit]
  
  //def resetBatchJob(job: JOB_INFO): Try[Unit]
  //def resetOnlineJob(job: JOB_INFO): Try[Unit]
  
  //def getRunningBatchJobSlot(job: JOB_INFO): Option[Int]
  //def getRunningOnlineJobSlot(job: JOB_INFO): Option[Int]
  
  def insertInBatchTable(jobName: JobInfo[Statement, InsertIn, Result], slot: Int, data: RowWithValue): Try[Unit]
  def insertInOnlineTable(jobName: JobInfo[Statement, InsertIn, Result], slot: Int, data: RowWithValue): Try[Unit]
  
  //def getOnlineJobState(job: JobInfo[Statement, InsertIn, Result]): Option[State]
  //def getBatchJobState(job: JobInfo[Statement, InsertIn, Result]): Option[State]
  
  def getOnlineJobData[T <: RowWithValue](jobname: String, slot: Int, result: T): Option[List[RowWithValue]]
  def getBatchJobData[T <: RowWithValue](jobname: String, slot: Int, result: T): Option[List[RowWithValue]]
    
  //def getQueryableTableIdentifiers: List[(String, TableIdentifier, Int)]
  // def getTableIdentifier(job: JOB_INFO): TableIdentifier
    
  def getLatestBatch(job: JOB_INFO): Option[Int] 
}

trait MergeApi extends LazyLogging {
  def merge(onlineData: RowWithValue, function: MonoidF[RowWithValue], batchData: RowWithValue): Try[Unit]
}

abstract class JobInfo[Statement, InsertIn, Result](
  val name: String,
  val numberOfBatchSlots: Int = 3,
  val numberOfOnlineSlots: Int = 2
  ) extends Serializable {
    
  var lock: Option[LockApi[Statement, InsertIn, Result]] = None
  def getLock(dbSession: DbSession[Statement, InsertIn, Result]): LockApi[Statement, InsertIn, Result]
      
  //def getBatchID(dbSession: DbSession[Statement, InsertIn, Result]): Option[BatchID] 
}

trait StateMonitoringApi[Statement, InsertIn, Result] extends LazyLogging {
  def getBatchJobState(job: JobInfo[Statement, InsertIn, Result], slot: Int): Option[State]
  def getOnlineJobState(job: JobInfo[Statement, InsertIn, Result], slot: Int): Option[State]
}

case class RunningJobExistsException(message: String) extends Exception(message)
case class NoRunningJobExistsException(message: String) extends Exception(message)
case class StatementExecutionError(message: String) extends Exception(message)
case class UnableToLockJobError(message: String) extends Exception(message)