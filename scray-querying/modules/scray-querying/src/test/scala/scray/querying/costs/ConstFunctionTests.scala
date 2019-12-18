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


@RunWith(classOf[JUnitRunner])
class ConstFunctionTests extends WordSpec {
  "Const functions " should {
    "use the factory " in {
      
      val factory = LinearQueryCostFuntionFactory
      
      ///val costfunction = factory.apply(null)
      
      
      
      assert(true)
      
    }
  }
  
}