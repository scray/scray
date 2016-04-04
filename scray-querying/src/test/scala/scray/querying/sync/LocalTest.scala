//package scray.querying.sync
//import scala.annotation.tailrec
//import org.junit.runner.RunWith
//import org.scalatest.WordSpec
//import org.scalatest.junit.JUnitRunner
//import com.datastax.driver.core.ResultSet
//import com.datastax.driver.core.SimpleStatement
//import scray.querying.description.Row
//import scray.querying.sync.types.DbSession
//import org.cassandraunit.utils.EmbeddedCassandraServerHelper
//import com.datastax.driver.core.Cluster
//import org.scalatest.BeforeAndAfter
//import scray.querying.sync.types.DataTable
//import scray.querying.sync.types.Column
//import com.datastax.driver.core.querybuilder.QueryBuilder
//import com.datastax.driver.core.querybuilder.Insert
//import com.datastax.driver.core.Statement
//import scray.querying.sync.cassandra.OnlineBatchSyncCassandra
//import scray.querying.sync.types.ArbitrarylyTypedRows
//import scray.querying.sync.types.ColumnWithValue
//import scray.querying.sync.cassandra.CassandraImplementation._
//import scray.querying.sync.cassandra.OnlineBatchSyncCassandra
//import scray.querying.sync.types.RowWithValue
//import shapeless._
//import syntax.singleton._
//import scray.querying.sync.types.ColumnWithValue
//import shapeless.ops.hlist._
//import scray.querying.sync._
//import org.scalatest.BeforeAndAfterAll
//import java.util.logging.LogManager
//import ch.qos.logback.classic.Level
//import ch.qos.logback.classic.Logger
//import org.slf4j.bridge.SLF4JBridgeHandler
//import scray.querying.sync.types.SyncTable
//import scray.querying.sync.types.State
//import scala.util.Try
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
//      val table = new OnlineBatchSyncCassandra("andreas")
//      table.initJob(JobInfo("job56"), new SumTestColumns())
//
//      println(table.initJob(JobInfo("job56"), new SumTestColumns()).isSuccess)
////      if(table.createNewJob(JobInfo("job56"), new SumTestColumns()).isSuccess) {
////        fail
////      } 
//
//    }
//  }
//}