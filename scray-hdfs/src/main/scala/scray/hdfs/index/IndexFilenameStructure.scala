package scray.hdfs.index

import org.apache.hadoop.fs.FileSystem
import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.fs.Path
import java.util.Timer
import java.util.concurrent.locks.ReentrantLock
import java.util.TimerTask
import org.apache.hadoop.fs.LocatedFileStatus

class IndexFilenameStructure(fs: FileSystem, directory: String, updateInterval: Long = 600000L) extends ScrayIndexFilesDirectoryStructure {
  import IndexFilenameStructure.FileTypes._

  val lock = new ReentrantLock
  val combinedfiles = new ArrayBuffer[CombinedFiles]
  reread
  
  // install reader that re-scans the directory every updateInterval ms
  val timer = new Timer(s"Update-Timer for $directory", true).schedule(new TimerTask {
    override def run(): Unit = reread
  }, updateInterval, updateInterval)
  
  class SingleHDFSFile(path: String, filetype: FileTypes ) extends SingleFile {
    private val fileTime = {
      val basename = path.stripSuffix("." + filetype.toString())
      val position = basename.lastIndexOf("-")
      basename.substring(position + 1).toLong
    }
    override def getNameWithPath: String = path
    override def getType: String = filetype.toString()
    override def getFileTime: Long = fileTime
    
    override def toString(): String = s"{filename = ${path}, type = ${getType}}" 
  }
  
  class CombinedHDFSIndexFiles(val blobfile: SingleHDFSFile, val indexfile: SingleHDFSFile, val bloomfile: SingleHDFSFile) extends CombinedFiles {
    override def getFileSet: Set[SingleFile] = Set(blobfile, indexfile, bloomfile)
    override def compare(that: CombinedFiles): Int = blobfile.getFileTime.compare(that.getFileSet.head.getFileTime)
    
    override def toString(): String = s"CombinedHDFSIndexFiles(${getFileSet.toString()})"
  }
  
  override def getFiles: List[CombinedFiles] = {
    lock.lock()
    try {
      combinedfiles.toList
    } finally {
      lock.unlock()
    }
  }
  
  override def reread: Unit = {
    def checkAndInsert(file: LocatedFileStatus, filetype: FileTypes): Unit = {
      if(!combinedfiles.find(cf => cf.getFileSet.map(_.getNameWithPath).contains(file.getPath.toString())).isDefined) {
        val filebasename = file.getPath.toString.stripSuffix(filetype.toString)
        combinedfiles += (new CombinedHDFSIndexFiles(
            new SingleHDFSFile(filebasename + BLOB.toString(), BLOB),
            new SingleHDFSFile(filebasename + INDEX.toString(), INDEX),
            new SingleHDFSFile(filebasename + BLOOM.toString(), BLOOM)))
      }
    }
    val files = fs.listFiles(new Path(directory), true)
    lock.lock()
    try {
      combinedfiles.clear()
      while(files.hasNext()) {
        val file = files.next()
        if(file.isFile() && file.getPath.isAbsolute()) {
          file.getPath.getName match {
            case str: String if str.endsWith(INDEX.toString) => checkAndInsert(file, INDEX)
            case str: String if str.endsWith(BLOOM.toString) => checkAndInsert(file, BLOOM)
            case str: String if str.endsWith(BLOB.toString) => checkAndInsert(file, BLOB)
          }
        }
      }
      combinedfiles.sorted
    } finally {
      lock.unlock()
    }
  }
  
}

object IndexFilenameStructure {
  
  object FileTypes extends Enumeration {
    type FileTypes = Value
    val INDEX = Value("idx")
    val BLOOM = Value("bloom")
    val BLOB = Value("blob")
  }
  
  
}

