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
import com.fasterxml.jackson.core.JsonParser
import java.util.concurrent.LinkedBlockingQueue
import com.seeburger.bdq.spark.serializers.GenericKafkaKryoSerializer
import java.util.Date

class WriteToKafka extends WordSpec {
  
    "OnlineBatchSync " should {
    " throw exception if job already exists" in {
      val inputQueue = new LinkedBlockingQueue[Share]()
      var count = 0;
      
      new KafkaWriter(inputQueue).start()

      while(count < 1000000) {
        count = count + 1
        if((count % 5000) == 0) {
          println(s"Write Date: ${new Date()} Count: ${count}  Queue.size ${inputQueue.size()}" )
        }
        inputQueue.put(new Share("share1_" + Math.random(), None, None, None, None, None))
      }
      
      Thread.sleep(1000000)
    }
    }
  
}