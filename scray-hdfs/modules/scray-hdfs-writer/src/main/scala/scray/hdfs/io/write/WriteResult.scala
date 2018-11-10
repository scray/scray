package scray.hdfs.io.write

class WriteResult(
   val isClosed: Boolean,
   val message: String) {
  
  def this(message: String) {
    this(false, message)
  }
}