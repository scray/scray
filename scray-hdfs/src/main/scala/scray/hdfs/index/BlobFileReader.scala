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
  def getBlobForPosition(fs: FileSystem, blobfile: String, key: ArrayBytes, ti: TableIdentifier, position: Long): Option[Array[Byte]] = {
    logger.info(s"scanning file $blobfile for blob at position: $position")
    HDFSBlobResolver.getCachedBlob(key).orElse {
      // need to read the index to find this key
      val path = new Path(blobfile)
      val fileIS = fs.open(path)
      try {
        val dis = new DataInputStream(new BufferedInputStream(fileIS))
        if(!getAndCheckVersion(dis)) throw new IOException(s"Blobfile version for $blobfile is not 0001")
        fileIS.seek(position)
        try {
          val keylength = dis.readInt()
          try {
            val origkey = new Array[Byte](keylength)
            dis.read(origkey)
            val bloblength = dis.readInt()
            val origblob = new Array[Byte](bloblength)
            dis.read(origblob)
            HDFSBlobResolver.putBlobIntoCache(key, origblob)
          } catch {
            case eof: EOFException => 
              logger.warn(s"Unexpected end of index file $blobfile")
          }
        } catch {
          case eof: EOFException => // end of strem, this is o.k. for the first record position
        }
        HDFSBlobResolver.getCachedBlob(key)
      } finally {
        fileIS.close()
      }
    }
  }
  
  
}