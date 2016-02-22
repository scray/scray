package scray.querying.sync

import scray.querying.sync.types._
import com.datastax.driver.core.Cluster
import com.typesafe.scalalogging.slf4j.LazyLogging
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.SimpleStatement
import com.datastax.driver.core.RegularStatement
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Statement

abstract class OnlineBatchSync extends LazyLogging {

  /**
   * Generate and register tables for a new job.
   */
  def initJob[T <: DataColumns](jobName: String, numberOfBatches: Int, dataTable: DataColumns)

  /**
   * Lock table if it is used by another spark job.
   */
   def lockTable(jobName: String)
  
   /**
   * Unlock table to make it available for a new job.
   */
  def unlockTable[T <: SyncTableColumns](jobName: String)

}

class OnlineBatchSyncCassandra(dbHostname: String, dbSession: Option[DbSession[SimpleStatement, ResultSet]]) extends OnlineBatchSync {

  // Create or use a given DB session.
  val session = dbSession.getOrElse(new DbSession[SimpleStatement, ResultSet](dbHostname) {
    val cassandraSession = Cluster.builder().addContactPoint(dbHostname).build().connect()

    override def execute(statement: String): ResultSet = {
      cassandraSession.execute(statement)
    }

    def execute(statement: SimpleStatement): ResultSet = {
      cassandraSession.execute(statement)
    }
  })

  val syncTable: Table[SyncTableColumns] = new SyncTableEmpty("\"ABC\"")

  /**
   * Generate and register tables for a new job.
   */
  def initJob[T <: DataColumns](jobName: String, numberOfBatches: Int, dataTable: DataColumns) {
    
    createKeyspace(syncTable)
    
    // Create data tables and register them in sync table
    1 to numberOfBatches foreach { i =>

      // Create batch data tables
      session.execute(createSingleTableString(new DataTable(syncTable.keySpace, getBatchJobName(jobName, i), dataTable)))

      // Register batch table
      session.execute(QueryBuilder.insertInto(syncTable.keySpace, syncTable.tableName)
        .value(syncTable.columns.tablename.name, getBatchJobName(syncTable.keySpace + "." + jobName, i))
        .value(syncTable.columns.completed.name, false)
        .value(syncTable.columns.jobname.name, jobName)
        .value(syncTable.columns.online.name, false)
        .value(syncTable.columns.lock.name, false)
        .value(syncTable.columns.time.name, System.currentTimeMillis()).toString())
    }

    // Create and register online tables
    1 to 3 foreach { i =>
      // Create online data tables
      session.execute(createSingleTableString(new DataTable(syncTable.keySpace, getOnlineJobName(jobName, i), dataTable)))

      // Register online tables
      session.execute(QueryBuilder.insertInto(syncTable.keySpace, syncTable.tableName)
        .value(syncTable.columns.tablename.name, getOnlineJobName(syncTable.keySpace + "." + jobName, i))
        .value(syncTable.columns.completed.name, false)
        .value(syncTable.columns.jobname.name, jobName)
        .value(syncTable.columns.online.name, true)
        .value(syncTable.columns.lock.name, false)
        .value(syncTable.columns.time.name, System.currentTimeMillis()).toString())
    }
  }

  /**
   * Create base keyspace and table
   */
  def createKeyspace[T <: Columns](table: Table[T]): Unit = {
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS ${table.keySpace} WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3};")
    session.execute(createSingleTableString(table))
  }

  /**
   * Lock table if it is used by another spark job.
   */
  def lockTable(jobName: String) {
    logger.debug(s"Lock table for job: ${jobName} ")
    executeQuorum(QueryBuilder.update(syncTable.keySpace + "." + syncTable.tableName).`with`(QueryBuilder.set(syncTable.columns.lock.name, true)).where(QueryBuilder.eq(syncTable.columns.jobname.name, jobName)).onlyIf(QueryBuilder.eq(syncTable.columns.lock.name, false)))
  }
  
    /**
   * Unlock table to make it available for a new job.
   */
  def unlockTable[T <: SyncTableColumns](jobName: String) {
    logger.debug(s"Unlock table for job: ${jobName}")
    executeQuorum(QueryBuilder.update(syncTable.keySpace + "." + syncTable.tableName).`with`(QueryBuilder.set(syncTable.columns.lock.name, false)).where(QueryBuilder.eq(syncTable.columns.jobname.name, jobName)).onlyIf(QueryBuilder.eq(syncTable.columns.lock.name, true)))   
  }
  
   def isLocked[T <: SyncTableColumns](jobName: String) {
    logger.debug(s"Unlock table for job: ${jobName}")
    val res = execute(QueryBuilder.select().all().from(syncTable.keySpace, syncTable.tableName).where(
        QueryBuilder.eq(syncTable.columns.jobname.name, jobName)
        ).and(QueryBuilder.eq(syncTable.columns.lock.name, true)))  
    if(res.all().size() > 0) {
     true
    } else {
     false
    }
  }
  
  private def executeQuorum(statement: RegularStatement) = {
    println(statement.toString())
    logger.debug("Lock table: " + statement)
    val simpleStatement = new SimpleStatement(statement.toString())
    simpleStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);

    val rs = session.execute(simpleStatement);
    val row = rs.one();

    if (row.getBool("[applied]")) {
      logger.debug(s"Execute ${simpleStatement}");
    } else {
      logger.error(s"It is currently not possible to execute ${simpleStatement} ")
    }
  }
  
  private def execute(statement: RegularStatement) = {
    println(statement.toString())
    logger.debug("Lock table: " + statement)
    val simpleStatement = new SimpleStatement(statement.toString())
    simpleStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);

    session.execute(simpleStatement);
  }

  def createSingleTableString[T <: Columns](table: Table[T]): String = {
    val createStatement = s"CREATE TABLE IF NOT EXISTS ${table.keySpace + "." + table.tableName} (" +
      s"${table.columns.foldLeft("")((acc, next) => { acc + next.name + " " + next.getDBType + ", " })} " +
      s"PRIMARY KEY ${table.columns.primKey})"
    logger.debug(s"Create table String: ${createStatement} ")
    println(s"Create table String: ${createStatement} ")
    createStatement
  }

  private def getBatchJobName(jobname: String, nr: Int): String = { jobname + "_batch" + nr }
  private def getOnlineJobName(jobname: String, nr: Int): String = { jobname + "_online" + nr }

}