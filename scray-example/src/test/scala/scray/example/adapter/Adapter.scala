package scray.example.adapter

import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfter
import org.scalatest.BeforeAndAfterAll
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner
import scala.util.matching.Regex
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.ObjectMapper


  @RunWith(classOf[JUnitRunner])
class Adapter extends WordSpec with BeforeAndAfter with BeforeAndAfterAll {
  import TokenizerState._

  "OnlineBatchSync " should {
//    " detect start " in {
//      val adapter = new YahooStreamAdapter
//      
//      // Detect start
//      assert(adapter.pars('a', TokenizerState.WAIT_FOR_OBJECT) === WAIT_FOR_OBJECT)
//      assert(adapter.pars('(', TokenizerState.WAIT_FOR_OBJECT) === OBJECT_START_1)
//      assert(adapter.pars('{', TokenizerState.OBJECT_START_1) === COLLECT_OBJECT_DATA)
//    }
//    " detect end " in {
//      val adapter = new YahooStreamAdapter
//      
//      // Collect end
//      assert(adapter.pars('}', TokenizerState.COLLECT_OBJECT_DATA) === OBJECT_END_1)
//      assert(adapter.pars('}', TokenizerState.OBJECT_END_1) === OBJECT_END_2)
//      assert(adapter.pars('}', TokenizerState.OBJECT_END_2) === WAIT_FOR_OBJECT)
//    }
//    " pars string " in {
//      val adapter = new YahooStreamAdapter
//      
//      val x: Stream[Character] = Array[Character]('a', 'b', 'c', '(', '{', 'a').seq.view.toStream
//      val y = x.scanLeft((WAIT_FOR_OBJECT, 'a'))((state, char) => adapter.pars(char, state._1)).filter { x => x._1 == TokenizerState.COLLECT_OBJECT_DATA}
//      
//      y.scanLeft("")((acc, char) => acc + char).f

      
      
      
//      val dddd = "abcdefg".foldLeft(WAIT_FOR_OBJECT)((state, char) => adapter.pars(char, state))
//      
//      var state = WAIT_FOR_OBJECT
//        adapter.getStream.map(WAIT_FOR_OBJECT)((state, char) => adapter.pars(char, state)).filter(dd).map
//      
//      println(dddd)
//    }
    
    
//    " throw exception if job already exists" in {
//     val stream = new YahooStreamAdapter
//     val sb = new StringBuffer
//     val buffer = new Array[Char](10000);
//     
////     while(true) {
//
//       val sb = new StringBuffer
//       //val len = stream.getStream2
//
//       //println("\n\n" + sb.toString() + "\n\n")
//       //println(sb.toString().split("\\(\\{").foreach { x => x.split("\\}\\}")})
//       //println(sb.toString().split("\\(\\{").foreach { x => println(x)})
//
////       println(sb.toString().
////           replace("<html><head><script type='text/javascript'> document.domain='finance.yahoo.com'; </script> </head><body></body><script>try{parent.yfs_mktmcb(", "").
////           replace(");}catch(e){}</script><script>try{parent.yfs_u1f(", "").
////           replace(";}catch(e){}</script><script>try{parent.yfs_u1f({\"USDUSD=X\":{l10:\"1.0000\",a00:\"1.0000\",b00:\"1.0000\",g00:\"1.0000\",h00:\"1.0000\"}});}catch(e){}</script>", "").
////           replace(";}catch(e){}</script>", "").
////           replace("}}", ""))
//    
//       
//       //Thread.sleep(1000)
// //    }
//     //buffer.filter { x => ??? }foldLeft(0)((acc, n) => acc)
//// 
//     
//    }
    

    "extract json object" in {
      val string = "<script>try{parent.yfs_u1f({\"EOAN.F\":{l10:\"8.45\",a00:\"8.50\",b00:\"8.50\",g00:\"8.41\",h00:\"8.51\"}});}catch(e){}</script>ipt>"
      
      val pattern = new Regex("""\(\{.*?\)""")
      
      for(stockString <- pattern.findAllIn(string)) {
       assert(stockString.replaceAll("\\(", "").replace(")", "") == "{\"EOAN.F\":{l10:\"8.45\",a00:\"8.50\",b00:\"8.50\",g00:\"8.41\",h00:\"8.51\"}}")
      }
    }
    "parse json string" in {
      val jsonObject = "{\"EOAN.F\":{l10:\"8.45\",a00:\"8.50\",b00:\"8.50\",g00:\"8.41\",h00:\"8.51\"}}"
      import scala.collection.JavaConversions._
      
      val mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)
      
      import scray.example.adapter.Share
      val user = mapper.readValue(jsonObject, classOf[Share])
      
      val pattern = new Regex("""\(.*?\)""")
      for(stockString <- pattern.findAllIn(jsonObject)) {  
       println(stockString.replaceAll("\\(", "").replace(")", ""))
      }
    }
  }
}