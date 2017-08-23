package scray.hdfs.index

/**
 * represents a directory and its files
 * WARNING: this is subject to state (i.e. if s.o. changes which files 
 * are in the directory (add, delete, rename) then an object of this class
 * may not be up-to-date with the actual files in the directory. To update
 * call reread 
 */
trait ScrayIndexFilesDirectoryStructure {
  
  
  /**
   * returns a list of files, potentially sorted/ordered
   */
  def getFiles: List[CombinedFiles]
  
  /**
   * re-read the directory; 
   */
  def reread: Unit
}

/**
 * represents a single Scray file
 */
trait SingleFile {
  /**
   * the name and fully qualitfied path of the the file,
   * e.g. hdfs://10.11.24.21:8020/user/blaschwaetz/file.txt
   */
  def getNameWithPath: String
  
  /**
   * return the type of file, actually this is an enum
   */
  def getType: String
  
  /**
   * date of this file
   */
  def getFileTime: Long
}

/**
 * a combined set of files,
 * e.g. a combination of index, blob and bloom filter files 
 */
trait CombinedFiles extends Ordered[CombinedFiles] {
  /**
   *  returns a combined set of files,
   *  e.g. a combination of index, blob and bloom filter files 
   */
  def getFileSet: Set[SingleFile]
}
