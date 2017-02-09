package scray.common.key

import org.junit.runner.RunWith
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner
import scray.querying.sync.SyncTableBasicClasses.SyncTableRowEmpty

@RunWith(classOf[JUnitRunner])
class TestOrderedStringKey extends WordSpec {
  "OrderedStringKey " should {
    " generate key from arry of strings" in {
      
      val keyElements = Array("d", "a", "c", "b")
      val key = new OrderedStringKey(keyElements)
      
      assert(key.getKeyAsString == "a_b_c_d")
    }
  }  
}