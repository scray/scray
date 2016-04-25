package scray.loader.configparser

/**
 * A scray config object that has a method to read the config.
 * In principle this method can be executed any time, so this has
 * been designed as to be a reload.
 */
trait ReadableConfig[T <: ConfigProperties] { self =>
  
  /**
   * reads ConfigProperties from the main configuration object
   */
  def readConfig(config: ScrayConfiguration, old: T): Option[T]
}