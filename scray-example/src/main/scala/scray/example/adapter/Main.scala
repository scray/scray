package scray.example.adapter

import java.util.concurrent.LinkedBlockingQueue
import java.util.Date

object Main {
  def main(args: Array[String]) {
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
  }
}