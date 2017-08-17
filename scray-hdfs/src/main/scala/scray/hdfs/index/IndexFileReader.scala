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
import scray.hdfs.index.format.IndexFile
import scray.hdfs.index.HDFSBlobResolver

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
      
      logger.info("getIndexForKey:" + key)
      
      // create blobfile from indexfile
      val blobfile = indexfile.stripSuffix(INDEX.toString()) + BLOB.toString()
      // need to read the index to find this key
      val path = new Path(indexfile)
      val fileIS = fs.open(path)
      var idxCount = 0L
      try {
                
        val dis = new DataInputStream(new BufferedInputStream(fileIS, 2000000))
        val indexFile = IndexFile.apply.getReader(dis)
        
        if(!checkVersion(indexFile.getVersion)) throw new IOException(s"Indexfile version for $indexfile is not 0001")
        while(indexFile.hasNextRecord) {
            val indexRecord =  indexFile.getNextRecord.get
              val origkey = indexRecord.getKey
              logger.debug("Load key: " + new String(origkey) + " to local index at position " + idxCount)
              val position = indexRecord.getStartPosition
              val hashedKey = new ArrayBytes(HDFSBlobResolver.computeHash(origkey, ti))
              if(hashedKey == key) {
                logger.info(s"found key in index")
              }
              HDFSBlobResolver.putIntoIndexCache(hashedKey, blobfile, position)
              idxCount += 1L
        }
        logger.debug(s"Loaded ${idxCount} entries from ${indexfile} into memory")
        HDFSBlobResolver.getCachedIdxPos(key)
      } finally {
        fileIS.close()
      }
    }
  }
}
