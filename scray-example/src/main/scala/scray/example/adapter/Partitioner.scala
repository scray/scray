package scray.example.adapter

import kafka.producer.Partitioner
import kafka.utils.VerifiableProperties

class SimplePartitioner(properties: VerifiableProperties) extends Partitioner {
 
    def partition(key: Any, a_numPartitions: Int): Int =  {
        var partition = 0;
        val offset = 1;
        if (offset > 0) {
           partition = offset % a_numPartitions;
        }
       return partition;
  }
}