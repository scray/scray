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


trait OnlineBatchSyncA[Statement, InsertIn, Result] extends LazyLogging {

  def initJob[DataTableT <: ArbitrarylyTypedRows](job: JobInfo[Statement, InsertIn, Result]): Try[Unit]

  def startNextBatchJob(job: JobInfo[Statement, InsertIn, Result], dataTable: TableIdentifier): Try[Unit]
  def startNextOnlineJob(job: JobInfo[Statement, InsertIn, Result]): Try[Unit]
  
  def completeBatchJob(job: JobInfo[Statement, InsertIn, Result]): Try[Unit]
  def completeOnlineJob(job: JobInfo[Statement, InsertIn, Result]): Try[Unit]
    
  def getQueryableTableIdentifiers: List[(String, TableIdentifier, Int)]
  def getTableIdentifier(job: JobInfo[Statement, InsertIn, Result]): TableIdentifier 
}

trait OnlineBatchSyncB[Statement, InsertIn, Result] extends LazyLogging {

  type JOB_INFO = JobInfo[Statement, InsertIn, Result]
  
  def initJob[DataTableT <: ArbitrarylyTypedRows](job: JobInfo[Statement, InsertIn, Result], dataTable: DataTableT): Try[Unit]
  
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

abstract class JobInfo[Statement, InsertIn, Result](
  val name: String,
  val numberOfBatchSlots: Int = 3,
  val numberOfOnlineSlots: Int = 2
  ) extends Serializable {
  
  var batchID: Option[BatchID] = None
  var lock: LockApi[Statement, InsertIn, Result] = null
  def getLock(dbSession: DbSession[Statement, InsertIn, Result]): LockApi[Statement, InsertIn, Result]
      
  def this(name: String, batchID: BatchID) {
    this(name)
    this.batchID = Some(batchID)
  }
  
  def getBatchID(dbSession: DbSession[Statement, InsertIn, Result]): BatchID = {
    batchID.get
  }    
}

case class RunningJobExistsException(message: String) extends Exception(message)
case class NoRunningJobExistsException(message: String) extends Exception(message)
case class StatementExecutionError(message: String) extends Exception(message)
case class UnableToLockJobError(message: String) extends Exception(message)