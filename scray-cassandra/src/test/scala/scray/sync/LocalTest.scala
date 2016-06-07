//package scray.querying.sync
//
//import java.util.logging.LogManager
//
//import scala.annotation.tailrec
//
//import org.junit.runner.RunWith
//import org.scalatest.BeforeAndAfter
//import org.scalatest.BeforeAndAfterAll
//import org.scalatest.WordSpec
//import org.scalatest.junit.JUnitRunner
//
//import scray.common.serialization.BatchID
//import scray.querying.description.Row
//import scray.querying.sync._
//import scray.querying.sync.cassandra.CassandraImplementation._
//import scray.querying.sync.cassandra.OnlineBatchSyncCassandra
//import scray.querying.sync.cassandra.OnlineBatchSyncCassandra
//import scray.querying.sync.types.ArbitrarylyTypedRows
//import scray.querying.sync.types.Column
//import scray.querying.sync.types.ColumnWithValue
//import scray.querying.sync.types.ColumnWithValue
//import scray.querying.sync.types.DbSession
//import scray.querying.sync.types.RowWithValue
//import shapeless.ops.hlist._
//import shapeless.syntax.singleton._
//
//
//  @RunWith(classOf[JUnitRunner])
//class LocalTests extends WordSpec with BeforeAndAfter with BeforeAndAfterAll {
//
//  override def beforeAll() = {
//    LogManager.getLogManager().reset();
//  }
//  
//  /**
//   * Test columns
//   */
//  class SumTestColumns() extends ArbitrarylyTypedRows {
//    val sum = new Column[Long]("sum")
//
//    override val columns = sum :: Nil
//    override val primaryKey = s"(${sum.name})"
//    override val indexes: Option[List[String]] = None
//  }
//
//
//  "OnlineBatchSync " should {
//    " throw exception if job already exists" in {
//            // val table = new OnlineBatchSyncCassandra(dbconnection)
//      val table = new OnlineBatchSyncCassandra("andreas")
//
//      val job = JobInfo("JOB_100", new BatchID(1L,1L))
//
//      val sum = new ColumnWithValue[Long]("sum", 100)
//      val columns = sum :: Nil
//      val primaryKey = s"(${sum.name})"
//      val indexes: Option[List[String]] = None
//
////      table.initJob(JobInfo("JOB_100"), new RowWithValue(columns, primaryKey, indexes))
////      table.startNextBatchJob(JobInfo("JOB_100"))
////      table.completeBatchJob(JobInfo("JOB_100"))
////      
////      table.initJob(JobInfo("JOB_200"), new RowWithValue(columns, primaryKey, indexes))
////      table.startNextBatchJob(JobInfo("JOB_200"))
////      table.completeBatchJob(JobInfo("JOB_200"))
////      
////      table.initJob(JobInfo("JOB_300"), new RowWithValue(columns, primaryKey, indexes))
////      table.startNextBatchJob(JobInfo("JOB_300"))
////      table.completeBatchJob(JobInfo("JOB_300"))
//      
//      val batchId = new BatchID(System.currentTimeMillis(), System.currentTimeMillis())
//      table.initJob(JobInfo("JOB_400", batchId), new RowWithValue(columns, primaryKey, indexes))
//      table.startNextBatchJob(JobInfo("JOB_400", batchId))
//      table.completeBatchJob(JobInfo("JOB_400", batchId))
//
//      table.startNextBatchJob(JobInfo("JOB_400", batchId))
//
//      table.completeBatchJob(JobInfo("JOB_400", batchId))
////     
////      val results = List("JOB_100_batch1", "JOB_300_batch1", "JOB_200_batch1", "JOB_400_batch2")
////      table.getQueryableTableIdentifiers.contains()
////      table.getQueryableTableIdentifiers.map{tableIdentifier => println(tableIdentifier._2.tableId + "\t" + results.contains(tableIdentifier._2.tableId))}
//
//    }
//  }
//}