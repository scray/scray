package scray.common.key

import org.junit.runner.RunWith
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner
import scray.querying.sync.SyncTableBasicClasses.SyncTableRowEmpty

@RunWith(classOf[JUnitRunner])
class TestOrderedStringKey extends WordSpec {
  "OrderedStringKey " should {
    " generate key from array of strings" in {
      
      val keyElements = Array("d", "a", "c", "b")
      val key = new OrderedStringKey(keyElements)
      
      assert(key.getKeyAsString == "a_b_c_d")
    }
    " get hash code for different keys" in {

      val key1 = new OrderedStringKey(Array("d", "a", "c", "b"))
      val key2 = new OrderedStringKey(Array("d1", "a1", "c1", "b1"))

      assert(key1.hashCode() == -1293253427)
      assert(key2.hashCode() == -1106818755)

    }
    " ignore input ordering " in {
      val expectedKeyAsString = "a_b_c_d"
      
      val keyV1 = new OrderedStringKey(Array("a", "b", "c", "d"))
      val keyV2 = new OrderedStringKey(Array("b", "c", "d", "a"))
      val keyV3 = new OrderedStringKey(Array("c", "d", "a", "b"))
      val keyV4 = new OrderedStringKey(Array("d", "a", "b", "c"))

      assert(keyV1.getKeyAsString == expectedKeyAsString)
      assert(keyV2.getKeyAsString == expectedKeyAsString)
      assert(keyV3.getKeyAsString == expectedKeyAsString)
      assert(keyV4.getKeyAsString == expectedKeyAsString)

      
      assert(keyV1.hashCode() == -1293253427)
      assert(keyV2.hashCode() == -1293253427)
      assert(keyV3.hashCode() == -1293253427)
      assert(keyV4.hashCode() == -1293253427)
    }
  }  
}