package scray.sync.api

import java.io.InputStream
import java.io.OutputStream

trait VersionedDataApi {
  def getLatestVersion(dataSource: String, mergeKey: String): Option[VersionedData]
  def updateVersion(dataSource: String, mergeKey: String, version: Long, data: String)
  /**
   * Persist versioned data informations to local file system 
   */
  def persist(path: String)
  
  /**
   * Write versioned data informations to OutputStream
   */
  def persist(path: OutputStream)
  
  /**
   * Load versioned data informations from local file system
   */
  def load(path: String)
  
  /**
   * Load versioned data informations from given InputStream
   */
  def load(path: InputStream)
}