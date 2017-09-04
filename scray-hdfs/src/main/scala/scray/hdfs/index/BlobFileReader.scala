package scray.hdfs.index

import scray.querying.source.store.BlobResolver
import org.apache.hadoop.io.Writable
import scray.querying.description.TableIdentifier
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.io.DataOutputStream
import java.io.ByteArrayOutputStream
import de.greenrobot.common.hash.Murmur3F
import scala.collection.mutable.HashMap
import java.io.BufferedInputStream
import java.io.IOException
import java.io.EOFException
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import java.io.DataInputStream
import com.typesafe.scalalogging.slf4j.LazyLogging
import scray.hdfs.index.HDFSBlobResolver
import scray.hdfs.index.format.sequence.BlobFileReader

object BlobFileReader extends LazyLogging {
  import HDFSBlobResolver.ArrayBytes
  
  /**
   * checks the version for the blob file
   */
  private def getAndCheckVersion(dis: DataInputStream): Boolean = {
    val bytes = new Array[Byte](4)
    dis.read(bytes)
    // check version
    bytes(3) == 1 && bytes(0) == 0 && bytes(1) == 0 && bytes(2) == 0
  }
  
  
  /**
   * we read the blob from the specified index into RAM cache
   */
  def getBlobForPosition(fs: FileSystem, blobfile: String, key: ArrayBytes, keyAsString: String, ti: TableIdentifier, position: Long): Option[Array[Byte]] = {
    logger.info(s"scanning file $blobfile for blob at position: $position")
    HDFSBlobResolver.getCachedBlob(key).orElse {
      // need to read the index to find this key
      val blobReader = new BlobFileReader(blobfile)
      try {
        blobReader.get(keyAsString, position).map(blob => {
          HDFSBlobResolver.putBlobIntoCache(key, blob)
        })

        HDFSBlobResolver.getCachedBlob(key)
      } finally {
        blobReader.close
      }
    }
  }
  
  
}