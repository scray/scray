package scray.example.adapter

import java.util.concurrent.BlockingQueue

class KafkaOutputAdapter(val inputQueue: BlockingQueue[Share]) extends Thread {
  
  override def run() {
    while(true) {
      println(inputQueue.take())
    }
  }
}