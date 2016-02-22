package scray.querying.costs

import org.junit.runner.RunWith
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner
import scray.querying.description.SimpleRow
import scray.querying.description.TableIdentifier
import scray.querying.description.Column
import scray.querying.description.RowColumn
import com.twitter.util.Future
import scray.querying.description.Row
import scray.querying.description.ColumnOrdering
import com.twitter.util.Await
import com.twitter.concurrent.Spool
import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scray.querying.source.costs.LinearQueryCostFuntionFactory
import scray.querying.sync.OnlineBatchSyncCassandra
import scray.querying.sync.types.DataColumns



@RunWith(classOf[JUnitRunner])
class OnlineBatchSyncTests extends WordSpec {
  "OnlineBatchSync " should {
    " " in {
      val table = new OnlineBatchSyncCassandra("andreas", None)
      table.initJob("job55", 2, new DataColumns(1L))      
    }
    "find newest batch" in {
      val table = new OnlineBatchSyncCassandra("andreas", None)
      table.initJob("job55", 2, new DataColumns(1L))    

      table.lockTable("job55")
    }
    "lock and unlock table" in {
      val table = new OnlineBatchSyncCassandra("andreas", None)
      table.initJob("job55", 2, new DataColumns(1L))
      
      println(table.isLocked("job55"))
    }
    
  }
  
}