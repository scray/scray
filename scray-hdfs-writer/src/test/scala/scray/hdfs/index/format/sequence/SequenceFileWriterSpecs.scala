package scray.hdfs.index.format.sequence

import org.scalatest.WordSpec
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import scray.hdfs.index.format.sequence.SequenceFileWriter
import org.junit.Assert
import scray.hdfs.index.format.sequence.IdxReader
import scray.hdfs.index.format.sequence.BlobFileReader
import org.scalatest.Tag
import com.typesafe.scalalogging.LazyLogging

class SequenceFileWriterSpecs extends WordSpec with LazyLogging {

  object RequiresHDFS extends Tag("scray.hdfs.tags.RequiresHDFS")

  def getKey(v: Integer) = "id_" + v
  def getValue(v: Integer) = "data_" + v

  "SequenceFileWriter " should {
    " write and read data file " in {

      val writer = new SequenceFileWriter("target/SeqFilWriterTest")

      for (i <- 0 to 1000) {
        writer.insert(getKey(i), 100000, getValue(i).getBytes)
      }
      writer.close

      // Seek to sync-marker at byte 22497 and return next data element
      val reader = new BlobFileReader("target/SeqFilWriterTest.blob")
      val data = reader.get(getKey(904), 22497L)

      Assert.assertTrue(data.isDefined)
      Assert.assertEquals(getValue(904), new String(data.get))
    }
    " read idx" in {

      val writer = new SequenceFileWriter("target/SeqFilWriterTest")

      for (i <- 0 to 1000) {
        writer.insert(getKey(i), 100000, getValue(i).getBytes)
      }
      writer.close

      // Seek to sync-marker at byte 22497 and return next data element
      val reader = new IdxReader("target/SeqFilWriterTest.idx")

      Assert.assertEquals(reader.hasNext, true)
      Assert.assertEquals(reader.next.isDefined, true)
      Assert.assertEquals(reader.next.get.getUpdateTime, 100000)
    }
    " read all index entries " in {
      val numDate = 1000 // Number of test data

      val writer = new SequenceFileWriter("target/IdxReaderTest")

      for (i <- 0 to numDate) {
        writer.insert(getKey(i), i, getValue(i).getBytes)
      }
      writer.close

      // Seek to sync-marker at byte 22497 and return next data element
      val reader = new IdxReader("target/IdxReaderTest.idx")

      Assert.assertEquals(reader.hasNext, true)

      for (i <- 0 to numDate) {
        val data = reader.next()

        Assert.assertTrue(data.isDefined)
        Assert.assertEquals(getKey(i), data.get.getKey.toString())
        Assert.assertEquals(i, data.get.getUpdateTime)
      }
    }
    " get data to given index entry " in {
      val conf = new Configuration
      val fs = FileSystem.getLocal(conf)

      val numDate = 1000 // Number of test data

      val writer = new SequenceFileWriter("target/IdxReaderTest")

      for (i <- 0 to numDate) {
        writer.insert(getKey(i), i, getValue(i).getBytes)
      }
      writer.close

      val idxReader = new IdxReader("target/IdxReaderTest.idx")
      val blobReader = new BlobFileReader("target/IdxReaderTest.blob")

      // Read whole index file and check if corresponding data exists
      Assert.assertEquals(idxReader.hasNext, true)

      for (i <- 0 to numDate) {
        val idx = idxReader.next()
        val data = blobReader.get(idx.get.getKey.toString(), idx.get.getPosition)

        Assert.assertTrue(data.isDefined)
        Assert.assertEquals(new String(data.get), getValue(i))
      }
    }
    " print keys " taggedAs (RequiresHDFS) in {

      val idxReader = new IdxReader("hdfs://192.168.0.201:8020/bdq-blob/.idx")

      while (idxReader.hasNext) {
        val idx = idxReader.next()
        println(idx.get.getKey)
      }
    }
  }
}