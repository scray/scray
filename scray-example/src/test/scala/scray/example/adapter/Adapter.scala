//package scray.example.adapter
//
//import org.junit.runner.RunWith
//import org.scalatest.BeforeAndAfter
//import org.scalatest.BeforeAndAfterAll
//import org.scalatest.WordSpec
//import org.scalatest.junit.JUnitRunner
//import scala.util.matching.Regex
//import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
//import com.fasterxml.jackson.module.scala.DefaultScalaModule
//import com.fasterxml.jackson.databind.ObjectMapper
//import com.fasterxml.jackson.core.JsonParser
//import java.util.concurrent.LinkedBlockingQueue
//import com.seeburger.bdq.spark.serializers.GenericKafkaKryoSerializer
//
//@RunWith(classOf[JUnitRunner])
//class Adapter extends WordSpec with BeforeAndAfter with BeforeAndAfterAll {
//  
//  
//  "OnlineBatchSync " should {
//    " throw exception if job already exists" in {
//      val inputQueue = new LinkedBlockingQueue[Share]()
//      
//      new StartKafkaServer
//      new StartSpark
//      
//      Thread.sleep(5000)
//      new YahooStreamAdapter(inputQueue).start()
//      new KafkaOutputAdapter(inputQueue).start()
//
//      Thread.sleep(1000000)
//    }
//    "serialize and deserialize " in {
//      val value = 1.5f 
//      val share = new Share("Share42", None, Some(value), Some(value), Some(value), Some(value))
//      
//      val serializer = new GenericKafkaKryoSerializer[Share](null)
//      
//      val bytes = serializer.toBytes(share)
//      val deserializedShare = serializer.fromBytes(bytes)
//      
//      assert(deserializedShare.name == "Share42")
//      assert(deserializedShare.l10 == None)
//      assert(deserializedShare.a00 == Some(1.5f))
//    }
//    "pars example" in {
//      import util.parsing.combinator.RegexParsers
//
//      val header = "<html><head><script type='text/javascript'> document.domain='finance.yahoo.com'; </script> </head><body></body>"
//      val line1 = "<script>try{parent.yfs_mktmcb({\"unixtime\":1462965657,\"open\":1462973400,\"close\":1462996800});}catch(e){}</script><script>try{parent.yfs_u1f({\"BAS.F\":{l10:\"68.48\",a00:\"68.53\",b00:\"68.51\",g00:\"68.32\",h00:\"69.20\"}});}catch(e){}</script><script>try{parent.yfs_u1f({\"DBK.F\":{l10:\"14.58\",a00:\"14.67\",b00:\"14.65\",g00:\"14.56\",h00:\"15.04\"}});}catch(e){}</script>"
//      val line2 = "<script>try{parent.yfs_u1f({\"SAP.F\":{l10:\"68.44\",a00:\"68.33\",b00:\"68.31\",g00:\"68.28\",h00:\"68.93\"}});}catch(e){}</script><script>try{parent.yfs_u1f({\"EOAN.F\":{l10:\"8.14\",a00:\"8.13\",b00:\"8.13\",g00:\"8.06\",h00:\"8.69\"}});}catch(e){}</script><script>try{parent.yfs_u1f({\"ADS.F\":{l10:\"113.36\",a00:\"113.45\",b00:\"113.40\",g00:\"113.20\",h00:\"114.74\"}});}catch(e){}</script>"
//
//      val parser = new YahooStockStreamParser
//      assert(parser.parseAll(parser.start, header + line1 + line2).successful)
//
//    }
//    "parse smal message" in {
//      val message = "<script>try{parent.yfs_u1f({\"EOAN.F\":{a00:\"8.13\",b00:\"8.12\"}});}catch(e){}</script>"
//      
//      val parser = new YahooStockStreamParser
//      assert(parser.parseAll(parser.start, message).successful)
//    }
//  }
//}