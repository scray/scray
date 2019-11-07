package scray.hdfs.io.osgi

import java.net.URI
import java.util.HashMap
import java.util.UUID
import java.util.Map
import java.lang.Boolean

import org.apache.hadoop.io.Writable
import org.slf4j.LoggerFactory

import scray.hdfs.io.index.format.raw.RawFileReader
import scray.hdfs.io.index.format.sequence.RawValueFileReader
import scray.hdfs.io.read.FileParameter
import scray.hdfs.io.read.ReadService
import scray.hdfs.io.write.IHdfsWriterConstats.SequenceKeyValueFormat
import scray.hdfs.io.write.ScrayListenableFuture
import scray.hdfs.io.index.format.sequence.mapping.impl.OutputTextBytesWritable
import scray.hdfs.io.index.format.sequence.mapping.impl.OutputBlob
import scray.hdfs.io.index.format.sequence.mapping.impl.OutputTextText
import java.util.NoSuchElementException
import scray.hdfs.io.index.format.sequence.types.BlobKey
import scray.hdfs.io.index.format.sequence.types.Blob
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.Text
import scala.None
import scala.None
import scray.hdfs.io.index.format.sequence.SequenceKeyValueFileReader
import scray.hdfs.io.index.format.sequence.SequenceKeyValueFileReader
import scray.hdfs.io.write.ScrayListenableFuture
import java.util.AbstractMap
import scray.hdfs.io.write.ScrayListenableFuture

class ReadServiceImpl extends ReadService {

  private val logger = LoggerFactory.getLogger(classOf[WriteServiceImpl])
  private val reader = new HashMap[String, RawFileReader]()
  private val sequenceFileReaderMetadata = new HashMap[UUID, SequenceKeyValueFileReader[Writable, Writable]];

  def getFileList(path: String, user: String, password: Array[Byte]): ScrayListenableFuture[java.util.List[FileParameter]] = {
    try {
      if (reader.get(this.getAuthority(path)) == null) {
        reader.put(this.getAuthority(path), new RawFileReader(path, user))
      }
      val result = reader.get(this.getAuthority(path)).getFileList(path).get
      return new ScrayListenableFuture[java.util.List[FileParameter]](result)
    } catch {
      case e: Throwable => {
        e.printStackTrace()
        new ScrayListenableFuture[java.util.List[FileParameter]](e)
      }
    }
  }

  def getInputStream(path: String, user: String, password: Array[Byte]): ScrayListenableFuture[java.io.InputStream] = {
    try {
      if (reader.get(this.getAuthority(path)) == null) {
        reader.put(getAuthority(path), new RawFileReader(path, user))
      }

      val stream = reader
        .get(getAuthority(path))
        .read(path)
      new ScrayListenableFuture(stream)
    } catch {
      case e: Throwable => {
        e.printStackTrace()
        new ScrayListenableFuture(e)
      }
    }
  }

  def deleteFile(path: String, user: String, password: Array[Byte]): ScrayListenableFuture[String] = {
    try {
      if (reader.get(this.getAuthority(path)) == null) {
        reader.put(getAuthority(path), new RawFileReader(path, user))
      }

      val stream = reader
        .get(getAuthority(path))
        .deleteFile(path)
      new ScrayListenableFuture(path)
    } catch {
      case e: Throwable => {
        e.printStackTrace()
        new ScrayListenableFuture(e)
      }
    }
  }

  def readFullSequenceFile(path: String, format: SequenceKeyValueFormat, user: String, password: Array[Byte]): UUID = {
    val id = UUID.randomUUID()
    format match {
      case SequenceKeyValueFormat.SEQUENCEFILE_INDEXVALUE_BLOB    => sequenceFileReaderMetadata.put(id, new SequenceKeyValueFileReader(path, new OutputBlob))
      case SequenceKeyValueFormat.SEQUENCEFILE_TEXT_BYTESWRITABLE => sequenceFileReaderMetadata.put(id, new SequenceKeyValueFileReader(path, new OutputTextBytesWritable))
      case SequenceKeyValueFormat.SEQUENCEFILE_TEXT_TEXT          => sequenceFileReaderMetadata.put(id, new SequenceKeyValueFileReader(path, new OutputTextText))
    }

    id
  }

  def hasNextSequenceFilePair(id: java.util.UUID): ScrayListenableFuture[Boolean] = {
    try {
      val hasNext = this.getReader(id)
      .hasNext
      new ScrayListenableFuture[Boolean](hasNext)
    } catch {
      case e: Exception => {
        new ScrayListenableFuture[Boolean](e)
      }
    }
  }

  def getNextSequenceFilePair(id: UUID): ScrayListenableFuture[Map.Entry[String, Array[Byte]]] = {

    var result: ScrayListenableFuture[Map.Entry[String, Array[Byte]]] = null

    this.getReader(id).next()
      .map(kv => {

        // Get key
        if (kv._1.isInstanceOf[Text]) {
          val key = new String(kv._1.asInstanceOf[Text].copyBytes())

          // Get value. Depending on type
          if (kv._2.isInstanceOf[BytesWritable]) {
            kv._2.asInstanceOf[BytesWritable].copyBytes()

            result = new ScrayListenableFuture(new AbstractMap.SimpleEntry[String, Array[Byte]](
              key,
              kv._2.asInstanceOf[BytesWritable].copyBytes()))
          } else if (kv._2.isInstanceOf[Text]) {

            result = new ScrayListenableFuture(new AbstractMap.SimpleEntry[String, Array[Byte]](
              key,
              kv._2.asInstanceOf[Text].toString().getBytes))

          } else {
            val errorMessage = s"Unkonwn class for SequenceFile value ${kv._2.getClass.getName}. " +
              "Implemented classes are org.apache.hadoop.io.BytesWritable and org.apache.hadoop.io.Text"
            logger.warn(errorMessage)
            result = new ScrayListenableFuture[Map.Entry[String, Array[Byte]]](new RuntimeException(errorMessage))
          }
        } else {
          val errorMessage = s"Unkonwn class for SequenceFile key ${kv._1.getClass.getName}. " +
            "Implemented classes are org.apache.hadoop.io.BytesWritable and org.apache.hadoop.io.Text"
          logger.warn(errorMessage)
          result = new ScrayListenableFuture[Map.Entry[String, Array[Byte]]](new RuntimeException(errorMessage))
        }
      })
      result
  }

 override def close(id: UUID) {
   this.getReader(id).close
 }
 
  private def get() = {

  }

  private def getReader(resource: UUID): SequenceKeyValueFileReader[Writable, Writable] = {
    if (sequenceFileReaderMetadata.get(resource) != null) {
      sequenceFileReaderMetadata.get(resource)
    } else {
      logger.error(s"No reader with id ${resource}. To create a reader call readFullSequenceFile(...) first.")
      throw new RuntimeException(s"No reader with id ${resource}. To create a reader call readFullSequenceFile(...) first.")
    }
  }
  private def getAuthority(path: String): String = {
    val uri = new URI(path)
    uri.getAuthority
  }
}