package scray.querying.source.store

trait BlobResolver[T] {
  
  /**
   * returns a Blob for the given key
   */
  def getBlob(key: T): Option[Array[Byte]] 
  
}