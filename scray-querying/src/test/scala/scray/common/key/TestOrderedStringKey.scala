package scray.common.key

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream

import org.junit.runner.RunWith
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner

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
    " be equal for the same input " in {
     
      val keyV1 = new OrderedStringKey(Array("a", "b", "c", "d"))
      val keyV2 = new OrderedStringKey(Array("b", "c", "d", "a"))
      
      assert(keyV1.equals(keyV2))
    }
    " be unequal for different input " in {
      
      val keyV1 = new OrderedStringKey(Array("a0", "b0", "c0", "d0"))
      val keyV2 = new OrderedStringKey(Array("b1", "c1", "d1", "a1"))
      
      assert( ! keyV1.equals(keyV2))
    }
    " be unequal for different classes " in {
      
      val keyV1 = new OrderedStringKey(Array("a0", "b0", "c0", "d0"))
      
      assert( ! keyV1.equals("abcd"))
    }
    " serialize and deserialize key " in {
      
      val key = new OrderedStringKey(Array("a", "b", "c", "d"))

      val keySerialized = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(keySerialized)
      oos.writeObject(key)
      oos.close()
      
      val inputStream = new ByteArrayInputStream(keySerialized.toByteArray())
      val objectInputStream = new ObjectInputStream(inputStream)
      val keyDeserialized = objectInputStream.readObject().asInstanceOf[OrderedStringKey]
      
      assert(keyDeserialized.version == 1)
      assert(keyDeserialized.getKeyAsString == "a_b_c_d")
    }
    " use key creation interface " in {
           
      class KeyHashComperator1(val keygenerator: KeyGenerator[String]) {
        
        def compare(value: String)= {
          assert(keygenerator(value).hashCode == 2987074)
        }
      }
      
      class KeyHashComperator2(val keygenerator: KeyGenerator[Array[String]]) {
        
        def compare(value: Array[String])= {
          assert(keygenerator(value).hashCode == -1293253427)
        }
      }

      new KeyHashComperator1(StringKey).compare("abcd")
      new KeyHashComperator2(OrderedStringKeyGenerator).compare(Array("a", "b", "c", "d"))
    }
    
  }  
}