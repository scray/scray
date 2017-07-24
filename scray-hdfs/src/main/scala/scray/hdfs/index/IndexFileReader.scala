package scray.hdfs.index

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.BufferedInputStream
import java.io.DataInputStream
import java.io.IOException
import java.io.EOFException
import com.typesafe.scalalogging.slf4j.LazyLogging
import scray.querying.description.TableIdentifier
import java.util.Arrays

/**
 * reads an index file from HDFS
 */
object IndexFileReader extends LazyLogging {
  import IndexFilenameStructure.FileTypes._
  import HDFSBlobResolver.ArrayBytes
  
  /**
   * checks the version for the index file
   */
  private def getAndCheckVersion(dis: DataInputStream): Boolean = {
    val bytes = new Array[Byte](4)
    dis.read(bytes)
    // check version
    bytes(3) == 1 && bytes(0) == 0 && bytes(1) == 0 && bytes(2) == 0
  }
  
  
  /**
   * we generally read the whole file at once
   * and then extract the position from RAM
   */
  def getIndexForKey(fs: FileSystem, indexfile: String, key: ArrayBytes, ti: TableIdentifier): Option[(String, Long)] = {
    HDFSBlobResolver.getCachedIdxPos(key).orElse {
      
      logger.info("getIndexForKey:" + key)
      
      // create blobfile from indexfile
      val blobfile = indexfile.stripSuffix(INDEX.toString()) + BLOB.toString()
      // need to read the index to find this key
      val path = new Path(indexfile)
      val fileIS = fs.open(path)
      var idxCount = 0L
      try {
        val dis = new DataInputStream(new BufferedInputStream(fileIS, 2000000))
        if(!getAndCheckVersion(dis)) throw new IOException(s"Indexfile version for $indexfile is not 0001")
        var eof = false
        while(!eof) {
          try {
            val keylength = dis.readInt()
            try {
              val origkey = new Array[Byte](keylength)
              dis.read(origkey)
              // if(wrong)
              //  logger.warn("key is: " + new String(origkey) + " at position " + idxCount)
              val position = dis.readLong()
              val hashedKey = new ArrayBytes(HDFSBlobResolver.computeHash(origkey, ti))
              if(hashedKey == key) {
                logger.info(s"found key in index")
              }
              HDFSBlobResolver.putIntoIndexCache(hashedKey, blobfile, position)
              idxCount += 1L
            } catch {
              case eo: EOFException => 
                logger.warn(s"Unexpected end of index file $indexfile")
                eof = true
            }
          } catch {
            case eo: EOFException => eof = true // end of strem, this is o.k. for the first record position
          }
        }
        logger.debug(s"Loaded ${idxCount} entries from ${indexfile} into memory")
        HDFSBlobResolver.getCachedIdxPos(key)
      } finally {
        fileIS.close()
      }
    }
  }
}
