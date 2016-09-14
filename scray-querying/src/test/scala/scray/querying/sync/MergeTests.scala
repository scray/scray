package scray.querying.sync


import org.junit.runner.RunWith
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner
import scray.querying.sync.SyncTableBasicClasses.SyncTableRowEmpty
import scalaz.Plus
import java.util.Calendar


@RunWith(classOf[JUnitRunner])
class MergeTests extends WordSpec {
  
  "Merge-Api " should {
    " sum two values" in {
      
      val liveStream = ("A", 2) :: ("B", 3) ::  Nil
      val batchResult = (key: String) => {
         val values = Map("A" -> 42, "B" -> 128)
         values.get(key).get
      }
      
      val plus = (a: Int, b: Int) => a + b  
      
      assert(liveStream.map(Merge.merge[String, Int](_, plus, batchResult)) === List(44, 131))
    }
    " filter dates " in {
      
      // Filter all data which were produced after 1473866500
      
      val now = Calendar.getInstance.getTime
      now.setTime(1473866500000L)
      println(now)
      
      val past = Calendar.getInstance.getTime
      past.setTime(1473866200000L)
      println(past)
      
      val future = Calendar.getInstance.getTime
      future.setTime(1473866700000L)
      println(future)
      
      val timeFilter = new TimeFilter(now) 
      val imputStream =  (past, "dataA") :: (future, "dataB") :: Nil
 
      val results = imputStream.map(values => timeFilter.filter(values._1, values)).flatten
      
      assert(results.size == 1)
      assert(results.contains((past, "dataA")))
    }
   }
}
