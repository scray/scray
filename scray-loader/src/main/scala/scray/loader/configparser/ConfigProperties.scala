package scray.loader.configparser

/**
 * Marker trait for proerties that can be checked for new properties being updates
 */
trait ConfigProperties {
  
  /**
   * check, if the new properties equal the original ones.
   * Returns true if the new properties are different from the original ones. False otherwise.
   */
  def needsUpdate(newProperties: Option[ConfigProperties]): Boolean =
    newProperties.map { _ != this }.getOrElse(true)
}
