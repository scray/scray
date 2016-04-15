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



abstract class OnlineBatchSync[Statement, InsertIn, Result] extends LazyLogging {

  type JOB_INFO = JobInfo[Statement, InsertIn, Result]
  
  /**
   * Generate and register tables for a new job.
   */
  def initJob[DataTableT <: ArbitrarylyTypedRows](job: JobInfo[Statement, InsertIn, Result], dataTable: DataTableT): Try[Unit]
  
  def startNextBatchJob(job: JobInfo[Statement, InsertIn, Result]): Try[Unit]
  def startNextOnlineJob(job: JOB_INFO): Try[Unit]
  
  def completeBatchJob(job: JOB_INFO): Try[Unit]
  def completeOnlineJob(job: JOB_INFO): Try[Unit]
  
  def resetBatchJob(job: JOB_INFO): Try[Unit]
  def resetOnlineJob(job: JOB_INFO): Try[Unit]
  
  def getRunningBatchJobSlot(job: JOB_INFO): Option[Int]
  def getRunningOnlineJobSlot(job: JOB_INFO): Option[Int]
  
  def insertInBatchTable(jobName: JOB_INFO, slot: Int, data: RowWithValue): Try[Unit]
  def insertInOnlineTable(jobName: JOB_INFO, slot: Int, data: RowWithValue): Try[Unit]
  
  def getOnlineJobState(job: JOB_INFO, slot: Int): Option[State]
  def getBatchJobState(job: JOB_INFO, slot: Int): Option[State]
  
  def getOnlineJobData[T <: RowWithValue](jobname: String, slot: Int, result: T): Option[List[RowWithValue]]
  def getBatchJobData[T <: RowWithValue](jobname: String, slot: Int, result: T): Option[List[RowWithValue]]
    
  def getQueryableTableIdentifiers: List[(String, TableIdentifier, Int)]
  
  def getNewestOnlineSlot(job: JOB_INFO): Option[Int]
  def getNewestBatchSlot(job: JOB_INFO): Option[Int]  
  
  def getLatestBatch(job: JOB_INFO): Option[Int] 
}

abstract class JobInfo[Statement, InsertIn, Result](
  val name: String,
  val batchID: BatchID,
  val numberOfBatchSlots: Int = 3,
  val numberOfOnlineSlots: Int = 2
  ) extends Serializable {
    var lock: LockApi[Statement, InsertIn, Result] = null
    def getLock(dbSession: DbSession[Statement, InsertIn, Result]): LockApi[Statement, InsertIn, Result] 
}

case class RunningJobExistsException(message: String) extends Exception(message)
case class NoRunningJobExistsException(message: String) extends Exception(message)
case class StatementExecutionError(message: String) extends Exception(message)