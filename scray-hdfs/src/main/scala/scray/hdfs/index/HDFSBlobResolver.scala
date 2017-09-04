package scray.hdfs.index

import scray.querying.source.store.BlobResolver
import org.apache.hadoop.io.Writable
import scray.querying.description.TableIdentifier
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.io.DataOutputStream
import java.io.ByteArrayOutputStream
import de.greenrobot.common.hash.Murmur3F
import scala.collection.mutable.HashMap
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.FloatWritable
import org.apache.hadoop.io.ShortWritable
import org.apache.hadoop.io.BooleanWritable
import java.math.BigInteger
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.ObjectWritable
import java.util.UUID
import scray.hdfs.hadoop.UUIDWritable
import scray.hdfs.hadoop.URLWritable
import java.net.URL
import com.typesafe.scalalogging.slf4j.LazyLogging
import java.util.Arrays

class HDFSBlobResolver[T <: org.apache.hadoop.io.Text](ti: TableIdentifier, directory: String) extends BlobResolver[org.apache.hadoop.io.Text] with LazyLogging {
  import IndexFilenameStructure.FileTypes._
  import HDFSBlobResolver._
  
  private val fs = getFileSystem()
  private val directoryScanner = new IndexFilenameStructure(fs, directory)
  
  /**
   * return FileSystem object
   */
  private def getFileSystem(): FileSystem = {
    val config = new Configuration()
    config.set("fs.defaultFS", directory)
    FileSystem.get(config)
  }

  /**
   * reads index files until a match for the key has been found.
   * If that could be found, it would return the relevant SingleFile for blobs.
   */
  def readAllIndexesUntilKeyIsfound(key: ArrayBytes, files: List[CombinedFiles]): Option[(String, Long)] = {

		  //logger.info("readAllIndexesUntilKeyIsfo")

		  if(files.isEmpty) {
			  None
		  } else {
			  val headFiles = files.head.getFileSet
					  val indexFile = headFiles.find(sf => sf.getType == INDEX.toString())
					  val longOption = indexFile.flatMap { idxFile =>
					  IndexFileReader.getIndexForKey(fs, idxFile.getNameWithPath, key, ti)
			  }
			  longOption.orElse {
				  readAllIndexesUntilKeyIsfound(key, files.tail)
			  }
		  }
  }

  /**
   * reads index files 
   */
  
  def readAllIndexes(files: List[CombinedFiles]) : Unit = {
		  if(files.isEmpty) {
			  None
		  } else {
			  files.foreach(f => { val indexFile = f.getFileSet.find(sf => sf.getType == INDEX.toString());
			    indexFile.foreach(f => IndexFileReader.updateCache(fs, f.getNameWithPath, ti))
			  })
		  }
  }
  
  // correct algorithm would be:
  // check the bloom filter for newest entry of the key
  // 
  
  
  def getBlob(key: Text): Option[Array[Byte]] = {
    // logger.info("getBlob:" + key.toString())
    val hashedKey = new ArrayBytes(computeHash(key, ti))
        
    val blob = HDFSBlobResolver.getCachedBlob(hashedKey).orElse {
      val files = directoryScanner.getFiles
    logger.info(s"known files: ${files}")
      // scan index-cache for existing entries
      HDFSBlobResolver.getCachedIdxPos(hashedKey).flatMap { filepos =>
        BlobFileReader.getBlobForPosition(fs, filepos._1, hashedKey, key.toString(), ti, filepos._2)
      }.orElse {
        // js - put all keys into cache
        readAllIndexes(files)
        return HDFSBlobResolver.getCachedBlob(hashedKey)
        // if we did not find the key, we need to find an index which contains it
        /* readAllIndexesUntilKeyIsfound(hashedKey, files).flatMap { filepos =>
          logger.info(s"Found key: ${key} in index")
          BlobFileReader.getBlobForPosition(fs, filepos._1, hashedKey, ti, filepos._2)
        } */
        
      }
    }
    // logger.info(s"Test result = $blob")
    blob
  }
}

object HDFSBlobResolver extends LazyLogging {
  
  // cache for index and positions
  val indexlock = new ReentrantReadWriteLock()
  var filenumber: Short = 0
  val idxmap = new HashMap[ArrayBytes, (Short, Long)]()
  val filesMap = new HashMap[Short, String]()
  val filesMapReversed = new HashMap[String, Short]()
  
  // cache for blobs
  val lock = new ReentrantReadWriteLock()
  val blobmap = new HashMap[ArrayBytes, Array[Byte]]
  
  
  //test
  def getAnyCachedIdxPos(): (scray.hdfs.index.HDFSBlobResolver.ArrayBytes,String, Long) = {
    indexlock.readLock().lock()
    try {
      val a = idxmap.iterator.next()
      
      logger.info(a.toString())
      
      (a._1, filesMap.get(a._2._1).get, a._2._2) 
      
      //get(key).map(pos => (filesMap.get(pos._1).get, pos._2))
    } finally {
      indexlock.readLock().unlock()
    }
  }
  
  
  def getCachedIdxPos(key: ArrayBytes): Option[(String, Long)] = {
    logger.debug(s"Request entry for key ${key} \t ${idxmap.get(key)}")
    indexlock.readLock().lock()
    try {
      idxmap.get(key).map(pos => (filesMap.get(pos._1).get, pos._2))
    } finally {
      indexlock.readLock().unlock()
    }
  }
  
  def putIntoIndexCache(key: ArrayBytes, blobfileName: String, position: Long): Unit = {
    indexlock.writeLock().lock()
    try {
      val fileNameNumber = filesMapReversed.get(blobfileName).getOrElse {
        val currfilenumber = filenumber
        filesMap += ((filenumber, blobfileName))
        filesMapReversed += ((blobfileName, filenumber))
        filenumber = (filenumber + 1).toShort
        currfilenumber
      }
      idxmap.put(key, (fileNameNumber, position))
    } finally {
      indexlock.writeLock().unlock()
    }
  }

  def getCachedBlob(key: ArrayBytes): Option[Array[Byte]] = {
    lock.readLock().lock()
    try {
      blobmap.get(key)
    } finally {
      lock.readLock().unlock()
    }
  }
  
  def putBlobIntoCache(key: ArrayBytes, blob: Array[Byte]): Unit = {
    lock.writeLock().lock()
    try {
      blobmap.put(key, blob)
    } finally {
      lock.writeLock().unlock()
    }
  }

    // internally we use murmur3f (128Bits) to represent the Blob keys...
  def computeHash[T <: Writable](key: T, ti: TableIdentifier): Array[Byte] = {
    val hasher = new Murmur3F()
    val bos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(bos)
    key.write(dos)
    dos.writeUTF(ti.dbId)
    dos.writeUTF(ti.tableId)
    hasher.update(bos.toByteArray())
    // don't need to close these streams as close does nothing
    hasher.getValueBytesBigEndian
  }
  
//  // internally we use murmur3f (128Bits) to represent the Blob keys...
//  def computeHash(key: Array[Byte], ti: TableIdentifier): Array[Byte] = {
//    val hasher = new Murmur3F()
//    val bos = new ByteArrayOutputStream()
//    val dos = new DataOutputStream(bos)
//    dos.write(key)
//    dos.writeUTF(ti.dbId)
//    dos.writeUTF(ti.tableId)
//    hasher.update(bos.toByteArray())
//    // don't need to close these streams as close does nothing
//    hasher.getValueBytesBigEndian
//  }
  
  def byteTransformHadoopType(input: Text): Array[Byte] = input match {
    case t: Text => t.toString().getBytes("UTF-8")
  }
  
  
  def transformHadoopTypes(input: Any): Writable = input match {
    case writable: Writable => writable
    case str: String => new Text(str)
    case int: Int => new IntWritable(int)
    case int: Integer => new IntWritable(int)
    case lng: Long => new LongWritable(lng)
    case dbl: Double => new DoubleWritable(dbl)
    case flt: Float => new FloatWritable(flt)
    case srt: Short => new ShortWritable(srt)
    case bol: Boolean => new BooleanWritable(bol)
    case uid: UUID => new UUIDWritable(uid)
    case url: URL => new URLWritable(url)
    case bi: Array[Byte] => new BytesWritable(bi)
    case o: Object => new ObjectWritable(o)
    // match error otherwise
  } 
  
  class ArrayBytes(val bytes: Array[Byte])  {
    override def equals(obj: Any): Boolean = {
      val that = obj.asInstanceOf[ArrayBytes]
      if((bytes == null && that == null) || (bytes == null && that.bytes == null)) {
        true
      } else {
        if(bytes == null) {
          false
        } else {
          if(that == null || that.bytes == null) {
            false
          } else {
            Arrays.equals(bytes, that.bytes)
          }
        }
      }
    }
    override def hashCode(): Int = Arrays.hashCode(bytes)
  }
  
}
