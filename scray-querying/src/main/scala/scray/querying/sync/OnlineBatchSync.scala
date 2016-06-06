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

trait OnlineBatchSyncWithTableIdentifier[Statement, InsertIn, Result, Metadata] extends LazyLogging {

  /**
   * Prepare tables for this job.
   */
  def initJob[DataTableT <: ArbitrarylyTypedRows](job: JobInfo[Statement, InsertIn, Result, Metadata]): Try[Unit]

  /**
   * Start new batch job in next slot. If no job is running.
   */
  def startNextBatchJob(job: JobInfo[Statement, InsertIn, Result, Metadata], dataTable: TableIdentifier): Try[Unit]
  
   /**
   * Start new batch job in next slot. If no job is running.
   */
  def startNextOnlineJob(job: JobInfo[Statement, InsertIn, Result, Metadata]): Try[Unit]
  
  /**
   * Mark the given batch job as completed.
   */
  def completeBatchJob(job: JobInfo[Statement, InsertIn, Result, Metadata]): Try[Unit]
  
   /**
   * Mark the given online job as completed.
   */
  def completeOnlineJob(job: JobInfo[Statement, InsertIn, Result, Metadata]): Try[Unit]
    
  def getQueryableTableIdentifiers: List[(String, TableIdentifier, Int)]
  def getTableIdentifier(job: JobInfo[Statement, InsertIn, Result, Metadata]): TableIdentifier 
}

abstract class IOOperation[DataFlowType, DataFlowParameters <: Product] extends Function2[DataFlowType, DataFlowParameters, Unit]

trait OnlineBatchSync[Statement, InsertIn, Result, Metadata] extends LazyLogging {

  type JOB_INFO = JobInfo[Statement, InsertIn, Result, Metadata]
  
  def initJob[DataTableT <: ArbitrarylyTypedRows](job: JobInfo[Statement, InsertIn, Result, Metadata], dataTable: DataTableT): Try[Unit]
  
  def startInicialBatch(job: JOB_INFO, batchID: BatchID): Try[Unit]
  
  def startNextBatchJob(job: JobInfo[Statement, InsertIn, Result, Metadata]): Try[Unit]
  def startNextOnlineJob(job: JobInfo[Statement, InsertIn, Result, Metadata]): Try[Unit]
  
  def completeBatchJob(job: JobInfo[Statement, InsertIn, Result, Metadata]): Try[Unit]
  def completeOnlineJob(job: JobInfo[Statement, InsertIn, Result, Metadata]): Try[Unit]
   
  def insertInBatchTable(jobName: JobInfo[Statement, InsertIn, Result, Metadata], slot: Int, ioOperation: () => Try[Unit]): Try[Unit]
  
  //def insertInOnlineTable[DataType](jobName: JobInfo[Statement, InsertIn, Result, Metadata], slot: Int, data: DataType): Try[Unit]
  
  def getOnlineJobData[T <: RowWithValue](jobname: String, slot: Int, result: T): Option[List[RowWithValue]]
  def getBatchJobData[T <: RowWithValue](jobname: String, slot: Int, result: T): Option[List[RowWithValue]]
    
  def getLatestBatch(job: JOB_INFO): Option[Int] 
}

trait MergeApi extends LazyLogging {
  def merge(onlineData: RowWithValue, function: MonoidF[RowWithValue], batchData: RowWithValue): Try[Unit]
}

abstract class JobInfo[Statement, InsertIn, Result, Metadata](
  val name: String,
  val numberOfBatchSlots: Int = 3,
  val numberOfOnlineSlots: Int = 2,
  val metadata: Option[Metadata] = None
  ) extends Serializable {
    
  var lock: Option[LockApi[Statement, InsertIn, Result]] = None
  def getLock(dbSession: DbSession[Statement, InsertIn, Result, Metadata]): LockApi[Statement, InsertIn, Result]
      
}

trait StateMonitoringApi[Statement, InsertIn, Result, Metadata] extends LazyLogging {
  def getBatchJobState(job: JobInfo[Statement, InsertIn, Result, Metadata], slot: Int): Option[State]
  def getOnlineJobState(job: JobInfo[Statement, InsertIn, Result, Metadata], slot: Int): Option[State]
}

case class RunningJobExistsException(message: String) extends Exception(message)
case class NoRunningJobExistsException(message: String) extends Exception(message)
case class StatementExecutionError(message: String) extends Exception(message)
case class UnableToLockJobError(message: String) extends Exception(message)