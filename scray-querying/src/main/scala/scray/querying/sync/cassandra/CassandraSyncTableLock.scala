package scray.querying.sync.cassandra

import java.util.concurrent.TimeUnit

import scala.annotation.tailrec
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import com.datastax.driver.core.BatchStatement
import com.datastax.driver.core.Cluster
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Session
import com.datastax.driver.core.SimpleStatement
import com.datastax.driver.core.Statement
import com.datastax.driver.core.querybuilder.Insert
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.typesafe.scalalogging.slf4j.LazyLogging

import scray.querying.sync.JobInfo
import scray.querying.sync.StatementExecutionError
import scray.querying.sync.types.DbSession
import scray.querying.sync.types.LockApi
import scray.querying.sync.types.SyncTableBasicClasses
import scray.querying.sync.types.Table
import scray.querying.sync.UnableToLockJobError

class CassandraSyncTableLock (job: JobInfo[Statement, Insert, ResultSet], jobLockTable: Table[SyncTableBasicClasses.JobLockTable], 
  dbSession: DbSession[Statement, Insert, ResultSet], val timeOut: Int) extends LockApi[Statement, Insert, ResultSet](job, jobLockTable, dbSession) with LazyLogging {
  
  val timeBetweenRetries = 100 // ms
   
  class CassandraSessionBasedDBSession(cassandraSession: Session) extends DbSession[Statement, Insert, ResultSet](cassandraSession.getCluster.getMetadata.getAllHosts().iterator().next.getAddress.toString) {

    override def execute(statement: String): Try[ResultSet] = {
      try {
        val result = cassandraSession.execute(statement)
        if(result.wasApplied()) {
         Success(result)
       } else {
         Failure(new StatementExecutionError(s"It was not possible to execute statement: ${statement}. Error: ${result.getExecutionInfo}"))
       }
      } catch {
        case e: Exception => logger.error(s"Error while executing statement ${statement}" + e); Failure(e)
      }
    }

    def execute(statement: Statement): Try[ResultSet] = {
      try {
        val result = cassandraSession.execute(statement)
        if(result.wasApplied()) {
         Success(result)
       } else {
         Failure(new StatementExecutionError(s"It was not possible to execute statement: ${statement}. Error: ${result.getExecutionInfo}"))
       }
      } catch {
        case e: Exception => logger.error(s"Error while executing statement ${statement}" + e); Failure(e)
      }
    }

    def insert(statement: Insert): Try[ResultSet] = {
      try {
        val result = cassandraSession.execute(statement)
        if(result.wasApplied()) {
         Success(result)
       } else {
         Failure(new StatementExecutionError(s"It was not possible to execute statement: ${statement}. Error: ${result.getExecutionInfo}"))
       }
      } catch {
        case e: Exception => logger.error(s"Error while executing statement ${statement}" + e); Failure(e)
      }
    }

    def execute(statement: SimpleStatement): Try[ResultSet] = {
       try {
        val result = cassandraSession.execute(statement)
        if(result.wasApplied()) {
         Success(result)
       } else {
         Failure(new StatementExecutionError(s"It was not possible to execute statement: ${statement}. Error: ${result.getExecutionInfo}"))
       }
      } catch {
        case e: Exception => logger.error(s"Error while executing statement ${statement}" + e); Failure(e)
      }
    }
    
    def printStatement(s: Statement): String = {
      s match {
         case bStatement: BatchStatement => "It is currently not possible to execute : " + bStatement.getStatements
         case _                          => "It is currently not possible to execute : " + s
      }
    }
}
  
  val lockQuery = QueryBuilder.update(jobLockTable.keySpace, jobLockTable.tableName)
    .`with`(QueryBuilder.set(jobLockTable.columns.locked.name, true))
    .where(QueryBuilder.eq(jobLockTable.columns.jobname.name, job.name))
    .onlyIf(QueryBuilder.eq(jobLockTable.columns.locked.name, false))
      
  val unLockQuery = QueryBuilder.update(jobLockTable.keySpace, jobLockTable.tableName)
    .`with`(QueryBuilder.set(jobLockTable.columns.locked.name, false))
    .where(QueryBuilder.eq(jobLockTable.columns.jobname.name, job.name))
    .onlyIf(QueryBuilder.eq(jobLockTable.columns.locked.name, true))
  
  def lock(): Unit = {
    @tailrec 
    def tryToLock: Unit = {
      try {
        while(!tryLock()){
          logger.debug(s"Retry to get lock for job ${job.name} in ${timeBetweenRetries}ms")
          Thread.sleep(timeBetweenRetries)
        }
      } catch {
        case e: InterruptedException => tryToLock
        case e: Throwable => throw e
      }
    }
    tryToLock
  }
  
  def lockInterruptibly(): Unit = {
    if(Thread.interrupted()) {
      throw new InterruptedException(s"Lock aquisition for job ${job.name} was interupted.")
    }
    
    while(!tryLock()){}
  }
  
  def tryLock(time: Long, unit: java.util.concurrent.TimeUnit): Boolean = {
    if(Thread.interrupted()) {
      throw new InterruptedException(s"Lock aquisition for job ${job.name} was interupted.")
    }

    val endTime = System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(time, unit)

    @tailrec 
    def tryToLockR(): Boolean = {
      if(tryLock()) {
        true
      } else {
        if(System.currentTimeMillis() >= endTime) {
          logger.debug(s"Retry to get lock for job ${job.name} in ${timeBetweenRetries}ms. Give up in ${TimeUnit.SECONDS.convert((endTime - System.currentTimeMillis()), TimeUnit.MICROSECONDS)}")
          Thread.sleep(timeBetweenRetries)
          tryToLockR()
        } else {
          false
        }
      }
    }
    
    tryToLockR
  }
  def tryLock(): Boolean = {
    executeQuorum(lockQuery).isSuccess
  }
  
  def unlock(): Unit = {
    executeQuorum(unLockQuery)
  }
  
  def newCondition(): java.util.concurrent.locks.Condition = ???
  
  def lockJob: Try[Unit] = {
   Try(this.lock())
  }
  
  def unlockJob(): Try[Unit] = {
    Try(unlock())
  }
  
  def transaction(f: () => Try[Unit]): Try[Unit] = {
    logger.debug(s"Start transaction for job ${this.job.name}")
    if(this.tryLock(timeOut, TimeUnit.MILLISECONDS)) {
      f() match {
        case Success(_) => this.unlock(); Try()
        case Failure(ex) => {
          logger.error(s"Unable to execute Query. Release lock for job ${job.name}")
          this.unlock()
          Failure(ex)}
      } 
    } else {
     logger.warn(s"Unable to lock job ${job.name}")
     Failure(new UnableToLockJobError(s"Unable to lock job ${job.name}")) 
    }
  }
  
  def transaction[P1](f: (P1) => Try[Unit], p1: P1): Try[Unit] = {
    if(this.tryLock(timeOut, TimeUnit.MILLISECONDS)) {
      f(p1) match {
        case Success(_) => this.unlock(); Try()
        case Failure(ex) => {
          logger.error(s"Unable to execute Query. Release lock for job ${job.name} p1")
          this.unlock()
          Failure(ex)}
      } 
    } else {
     Failure(new UnableToLockJobError(s"Unable to lock job ${job.name}")) 
    }
  }
  
  def transaction[P1, P2](f: (P1, P2) => Try[Unit], p1: P1, p2: P2): Try[Unit] = {
    if(this.tryLock(timeOut, TimeUnit.MILLISECONDS)) {
      f(p1, p2) match {
        case Success(_) => this.unlock(); Try()
        case Failure(ex) => {
          logger.error(s"Unable to execute Query. Release lock for job ${job.name}")
          this.unlock()
          Failure(ex)}
      } 
    } else {
     Failure(new UnableToLockJobError(s"Unable to lock job ${job.name}")) 
    }
  }
  
  def transaction[P1, P2, P3](f: (P1, P2, P3) => Try[Unit], p1: P1, p2: P2, p3: P3): Try[Unit] = {
    if(this.tryLock(timeOut, TimeUnit.MILLISECONDS)) {
      f(p1, p2, p3) match {
        case Success(_) => this.unlock(); Try()
        case Failure(ex) => {
          logger.error(s"Unable to execute Query. Release lock for job ${job.name}")
          this.unlock()
          Failure(ex)}
      } 
    } else {
     Failure(new UnableToLockJobError(s"Unable to lock job ${job.name}")) 
    }
  }
  
  def transaction[P1, P2, P3, P4](f: (P1, P2, P3, P4) => Try[Unit], p1: P1, p2: P2, p3: P3, p4: P4): Try[Unit] = {
    if(this.tryLock(timeOut, TimeUnit.MILLISECONDS)) {
      f(p1, p2, p3, p4) match {
        case Success(_) => this.unlock(); Try()
        case Failure(ex) => {
          logger.error(s"Unable to execute Query. Release lock for job ${job.name}")
          this.unlock()
          Failure(ex)}
      } 
    } else {
     Failure(new UnableToLockJobError(s"Unable to lock job ${job.name}")) 
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
          logger.error(s"Error while executing statement: ${statement}.}")
          Failure(new StatementExecutionError(ex.getLocalizedMessage))
        } 
      }
  }
}

object CassandraSyncTableLock {
  def apply(
      job: JobInfo[Statement, Insert, ResultSet], 
      jobLockTable: Table[SyncTableBasicClasses.JobLockTable], 
      dbSession: DbSession[Statement, Insert, ResultSet],
      timeOut: Int) = {
    new CassandraSyncTableLock(job, jobLockTable, dbSession, timeOut)
  }
}