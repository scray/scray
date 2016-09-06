package scray.querying.sync

import java.util.{ Iterator => JIterator }

import scala.annotation.tailrec
import scala.util.Try

import com.typesafe.scalalogging.slf4j.LazyLogging

import scray.common.serialization.BatchID
import scray.querying.description.TableIdentifier
import scray.querying.sync.State.State
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
  
  /**
   * Get latest version for a given batch job.
   */
  def getBatchVersion(job: JobInfo[Statement, InsertIn, Result]): Option[TableIdentifier]
  
  /**
   * Get latest version for a given online job.
   */
  def getOnlineVersion(job: JobInfo[Statement, InsertIn, Result]): Option[TableIdentifier]
    
  def getQueryableTableIdentifiers: List[(String, TableIdentifier, Int)]
  def getTableIdentifierOfRunningJob(job: JobInfo[Statement, InsertIn, Result]): Option[TableIdentifier] 
}

abstract class IOOperation[DataFlowType, DataFlowParameters <: Product] extends Function2[DataFlowType, DataFlowParameters, Unit]

trait OnlineBatchSync[Statement, InsertIn, Result] extends LazyLogging {

  type JOB_INFO = JobInfo[Statement, InsertIn, Result]
  
  def initJob[DataTableT <: ArbitrarylyTypedRows](job: JobInfo[Statement, InsertIn, Result], dataTable: DataTableT): Try[Unit]
  
  def startInicialBatch(job: JOB_INFO, batchID: BatchID): Try[Unit]
  
  def startNextBatchJob(job: JobInfo[Statement, InsertIn, Result]): Try[Unit]
  def startNextOnlineJob(job: JobInfo[Statement, InsertIn, Result]): Try[Unit]
  
  def completeBatchJob(job: JobInfo[Statement, InsertIn, Result]): Try[Unit]
  def completeOnlineJob(job: JobInfo[Statement, InsertIn, Result]): Try[Unit]
   
  //def insertInBatchTable(jobName: JobInfo[Statement, InsertIn, Result], slot: Int, ioOperation: () => Try[Unit]): Try[Unit]
  
  //def insertInOnlineTable[DataType](jobName: JobInfo[Statement, InsertIn, Result], slot: Int, data: DataType): Try[Unit]
  
  def getOnlineJobData[T <: RowWithValue](jobname: String, slot: Int, result: T): Option[List[RowWithValue]]
  /**
   * Get all values of batch view.
   */
  def getBatchJobData[T <: RowWithValue](jobname: String, slot: Int, result: T): Option[List[RowWithValue]]
  
  /**
   * Get batch view data for a special key
   */
  def getBatchJobData[T <: RowWithValue, K](jobname: String, slot: Int, key: K, result: T): Option[RowWithValue]
    
  def getLatestBatch(job: JOB_INFO): Option[Int] 
}


abstract class JobInfo[Statement, InsertIn, Result](
  val name: String,
  val numberOfBatchSlots: Int = 3,
  val numberOfOnlineSlots: Int = 2,
  val dbSystem: String = "cassandra" // Defines the db system for the results of this job.
  ) extends Serializable {

  var lock: Option[LockApi[Statement, InsertIn, Result]] = None
  def getLock(dbSession: DbSession[Statement, InsertIn, Result]): LockApi[Statement, InsertIn, Result]
      
}

trait StateMonitoringApi[Statement, InsertIn, Result] extends LazyLogging {
  def getBatchJobState(job: JobInfo[Statement, InsertIn, Result], slot: Int): Option[State]
  def getOnlineJobState(job: JobInfo[Statement, InsertIn, Result], slot: Int): Option[State]
}

/**
 * Merge two elements.
 * @param element1 Key value pair of first operand.
 * @param element2 Function witch returns the second operand depending on the key of the first operand.
 * @param operator Operation to merge element1 and element2.
 */
object Merge {
  def merge[K, V](element1: (K, V), operator: (V, V) => V, element2: (K) => V): V = {
    operator(element1._2, element2(element1._1))
  }
}

case class RunningJobExistsException(message: String) extends Exception(message)
case class NoRunningJobExistsException(message: String) extends Exception(message)
case class StatementExecutionError(message: String) extends Exception(message)
case class UnableToLockJobError(message: String) extends Exception(message)