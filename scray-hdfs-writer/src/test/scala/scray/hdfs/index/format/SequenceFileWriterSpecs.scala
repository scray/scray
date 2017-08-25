package scray.hdfs.index.format

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.scalatest.WordSpec
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import scray.hdfs.index.format.sequence.SequenceFileWriter
import scray.hdfs.index.format.sequence.SequenceFileReader
import org.junit.Assert

class SequenceFileWriterSpecs extends WordSpec with LazyLogging {
  "SequenceFileWriter " should {
    " write and read data file " in {
        val  conf = new Configuration
        val fs = FileSystem.getLocal(conf)
        
        val key   = "id_"
        val value = "data_" 
        
        val writer = new SequenceFileWriter(conf, "target/SeqFilWriterTest.seq", Some(fs))
       
        for(i <- 0 to 1000) {
          writer.insert((key + i), 100000, (value +  i).getBytes)
        }
        writer.close
        

        // Seek to sync-marker at byte 22497 and return next data element
        val reader =  new SequenceFileReader(conf, "target/SeqFilWriterTest.seq", Some(fs))
        val data = reader.get(key + 904, 22497L)
       
        Assert.assertEquals((value + 904), new String(data))        
    }
   }
  
}