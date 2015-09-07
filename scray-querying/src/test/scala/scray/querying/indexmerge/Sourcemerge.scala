package scray.querying.indexmerge

import org.junit.runner.RunWith
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner
import scray.querying.description.RowColumn
import scray.querying.description.SimpleRow
import scala.collection.mutable.ArrayBuffer
import scray.querying.description.Column
import scray.querying.description.TableIdentifier
import com.twitter.util.Future
import com.twitter.concurrent.Spool
import scray.querying.description.Row
import scray.querying.source.LazyTestSource
import scray.querying.source.EagerCollectingQueryMappingSource
import scray.querying.source.IndexMergeSource
import scray.querying.source.MergeReferenceColumns
import scray.querying.description.IndexConfiguration
import scray.querying.queries.DomainQuery
import scray.querying.source.LazySource
import scray.querying.source.EagerSource
import scray.querying.source.EagerTestSource
import scray.querying.queries.DomainQuery
import com.twitter.concurrent.Spool.Cons
import com.twitter.util.Promise
import com.twitter.util.Await
import scala.annotation.tailrec


@RunWith(classOf[JUnitRunner])
class Sourcemerge extends WordSpec {
  
   "Sources" should {
    "deliver data" in {
      
      // Generate spool with data
      val ti = TableIdentifier("cassandra", "mytestspace", "mycf")
      val cols = 1.until(10).map(i => Column(s"bla$i", ti))
      val sr1 = SimpleRow(ArrayBuffer(RowColumn(cols(0), 1), RowColumn(cols(1), Set("A"))))  
      val sr2 = SimpleRow(ArrayBuffer(RowColumn(cols(2), 2), RowColumn(cols(1), Set("B"))))  
      val sr3 = SimpleRow(ArrayBuffer(RowColumn(cols(0), 3), RowColumn(cols(1), Set("C"))))
      val sr4 = SimpleRow(ArrayBuffer(RowColumn(cols(0), 4), RowColumn(cols(1), Set("D"))))
      
      val spool = Future(sr1 **:: sr2 **:: sr3 **:: Spool.empty[Row])
      val seq = Future(Seq(sr1, sr2, sr3, sr4))
      
      
      // Add spool to source
      val mainSource = new LazyTestSource(spool.get())
      val refereceSource = new EagerTestSource(seq.get())
      
      val main = MergeReferenceColumns[DomainQuery, Spool[Row], LazySource[DomainQuery]](mainSource, cols(1), IndexConfiguration(true, None, true, true, true, None))
      val reference = MergeReferenceColumns[DomainQuery, Seq[Row], EagerSource[DomainQuery]](refereceSource, cols(1), IndexConfiguration(true, None, true, true, true, None))
      
      val mappingSource  = new  IndexMergeSource(main, reference).request(new DomainQuery(null, null,  null, null, null, null, null, null))

     val expectedResults = Set[Row](sr1, sr2, sr3)

     @tailrec def checkResults(value : Set[Row], count: Int, spool: Spool[Row]): Boolean = spool match {
        case Spool.Empty  => value.size == count
        case _ =>
          import Sourcemerge.RowSetContainsChecker
          if(!value.rowIsContained(spool.head)) false else checkResults(value, count+1, Await.result(spool.tail))
      }
      assert(true === checkResults(expectedResults, 0, Await.result(mappingSource)))
    }
   }
}
object Sourcemerge {
     implicit class RowSetContainsChecker(rowset: Set[Row]) {
        def rowIsContained(row: Row) = {
          rowset.find { setrow =>
            !setrow.getColumns.find { col => setrow.getColumnValue(col) != row.getColumnValue(col)}.isDefined
          }.isDefined
        }
      }
           
  
}