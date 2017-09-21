package scray.hdfs.index

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.BufferedInputStream
import java.io.DataInputStream
import java.io.IOException
import java.io.EOFException
import scray.querying.description.TableIdentifier
import java.util.Arrays
import scray.hdfs.index.format.IndexFile
import scray.hdfs.index.HDFSBlobResolver
import scray.hdfs.index.format.sequence.IdxReader
import org.apache.hadoop.io.Text
import com.typesafe.scalalogging.LazyLogging

/**
 * reads an index file from HDFS
 */
object IndexFileReader extends LazyLogging {
  import IndexFilenameStructure.FileTypes._
  import HDFSBlobResolver.ArrayBytes

  /**
   * checks the version for the index file
   */
  private def checkVersion(version: Array[Byte]): Boolean = {
    // check version
    version(3) == 1 && version(0) == 0 && version(1) == 0 && version(2) == 0
  }

  /**
   * we generally read the whole file at once
   * and then extract the position from RAM
   */
  def getIndexForKey(fs: FileSystem, indexfile: String, key: ArrayBytes, ti: TableIdentifier): Option[(String, Long)] = {
    HDFSBlobResolver.getCachedIdxPos(key).orElse {
      updateCache(fs, indexfile, ti)
      HDFSBlobResolver.getCachedIdxPos(key)
    }
  }

  def updateCache(fs: FileSystem, indexfile: String, ti: TableIdentifier) = {
    
    logger.debug(s"Load idx from file ${indexfile}")
    val reader = new IdxReader(indexfile)
    // create blobfile from indexfile
    val blobfile = indexfile.stripSuffix(INDEX.toString()) + BLOB.toString()
    // need to read the index to find this key
    var idxCount = 0L
    try {
      while (reader.hasNext) {
          reader.next().map(indexRecord => {
          val origkey = indexRecord.getKey
          logger.debug("Load key: " + origkey + " to local index at position " + idxCount)
          val position = indexRecord.getPosition
          val hashedKey = new ArrayBytes(HDFSBlobResolver.computeHash(origkey, ti))
            
          HDFSBlobResolver.putIntoIndexCache(hashedKey, blobfile, position)
          idxCount += 1L
        })
      }
      logger.debug(s"Loaded ${idxCount} entries from ${indexfile} into memory")
      // HDFSBlobResolver.getCachedIdxPos(key)
    } catch {
      case e: Exception => {
        logger.error(s"Exception ${e.getMessage}")
        e.printStackTrace()
      }
    }
    
    finally {
      reader.close
    }
  }
}