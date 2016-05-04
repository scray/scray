package scray.example.adapter

import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfter
import org.scalatest.BeforeAndAfterAll
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner





  @RunWith(classOf[JUnitRunner])
class Adapter extends WordSpec with BeforeAndAfter with BeforeAndAfterAll {


  "OnlineBatchSync " should {
    " throw exception if job already exists" in {
     val stream = new YahooStreamAdapter
     val sb = new StringBuffer
     val buffer = new Array[Char](10000);
     
     while(true) {

       val sb = new StringBuffer
       val len = stream.getStream.read(buffer);
       println(len)
       sb.append(buffer, 0, len);

       
       println(sb.toString())
//       println(sb.toString().
//           replace("<html><head><script type='text/javascript'> document.domain='finance.yahoo.com'; </script> </head><body></body><script>try{parent.yfs_mktmcb(", "").
//           replace(");}catch(e){}</script><script>try{parent.yfs_u1f(", "").
//           replace(";}catch(e){}</script><script>try{parent.yfs_u1f({\"USDUSD=X\":{l10:\"1.0000\",a00:\"1.0000\",b00:\"1.0000\",g00:\"1.0000\",h00:\"1.0000\"}});}catch(e){}</script>", "").
//           replace(";}catch(e){}</script>", "").
//           replace("}}", ""))
    
       
       //Thread.sleep(1000)
     }
     //buffer.filter { x => ??? }foldLeft(0)((acc, n) => acc)
// 
     
    }
  }
}