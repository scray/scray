//package scray.querying.sync
//
//import scray.querying.sync.cassandra.OnlineBatchSyncCassandra
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
//import scray.querying.sync.cassandra.CassandraImplementation.RichBoolean
//import scala.util.Failure
//import scala.util.Success
//import java.io.FileOutputStream
//import java.io.ObjectOutputStream
//import java.io._;
//
//class IoTests  extends WordSpec {
//   
//  class SumTestColumns() extends ArbitrarylyTypedRows {
//    val testColumn1 = new Column[Long]("testColumn1")
//
//    override val columns = testColumn1 :: Nil
//    override val primaryKey = s"(${testColumn1.name})"
//    override val indexes: Option[List[String]] = None
//  }
//  
//   "SyncTable " should {
//    " be serialized " in {
//      EmbeddedCassandraServerHelper.startEmbeddedCassandra(EmbeddedCassandraServerHelper.CASSANDRA_RNDPORT_YML_FILE)
//      val table = new OnlineBatchSyncCassandra("127.0.0.1", EmbeddedCassandraServerHelper.getNativeTransportPort)
//      
//      assert(table.initJob[SumTestColumns](JobInfo("job55"), new SumTestColumns).isSuccess)
//      
//      val fileOut: FileOutputStream = new FileOutputStream("object.out");
//      val out: ObjectOutputStream = new ObjectOutputStream(fileOut);
//      out.writeObject(table);
////      out.close();
////      fileOut.close();
//    }
//    " be deserialized " in {
//      EmbeddedCassandraServerHelper.startEmbeddedCassandra(EmbeddedCassandraServerHelper.CASSANDRA_RNDPORT_YML_FILE)
//      val table = new OnlineBatchSyncCassandra("127.0.0.1", EmbeddedCassandraServerHelper.getNativeTransportPort)
//      
//      assert(table.initJob[SumTestColumns](JobInfo("job55"), new SumTestColumns).isSuccess)
//      
////      val fileIn: FileInputStream = new FileInputStream("object.out");
////      val in: ObjectInputStream = new ObjectInputStream(fileIn);
////      in.close();
//    }
//   }
//  
//}