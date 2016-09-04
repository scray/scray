package scray.querying.sync


import org.junit.runner.RunWith
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner
import scray.querying.sync.SyncTableBasicClasses.SyncTableRowEmpty
import scalaz.Plus


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
   }
}
