package scray.querying.monitoring

import java.lang.{Double => JDouble, Long => JLong}
import javax.management.{ AttributeList, DynamicMBean, MBeanInfo, MBeanAttributeInfo,
  Attribute, MBeanOperationInfo, MBeanParameterInfo}
import scala.util.Random
import scala.collection.convert.WrapAsScala._
import scray.querying.Registry
import scray.querying.caching.MonitoringInfos
import scray.querying.caching.KeyValueCache
import scray.querying.caching.Cache
import com.typesafe.scalalogging.slf4j.LazyLogging


class MonitoringInfoBean(name: String) extends DynamicMBean with LazyLogging {

  val changeCacheSizeOperationName : String = "Request: change cache size"

  def getValueSizeGB() : Double = {
    val cacheCounter = Registry.getCacheCounter(name)
    if (!cacheCounter.toString().startsWith("None"))
      cacheCounter.get.sizeGB
    else 0.0d
  }

  def getEntries() : Long = {
    val cacheCounter = Registry.getCacheCounter(name)
    if (!cacheCounter.toString().startsWith("None"))
      cacheCounter.get.entries
    else 0L
  }

  def getCurrentSize() : Long = {
    val cacheCounter = Registry.getCacheCounter(name)
    if (!cacheCounter.toString().startsWith("None"))
      cacheCounter.get.currentSize
    else 0L
  }

  def getFreeSize(): Long = {
    val cacheCounter = Registry.getCacheCounter(name)
    if (!cacheCounter.toString().startsWith("None"))
      cacheCounter.get.freeSize
    else 0L
  }


  override def getAttribute(attribute: String): Object = {
    if (attribute == "sizeGB") {
      new JDouble(getValueSizeGB())
    } else {
      if (attribute == "entries") {
        new JLong(getEntries())
      } else {
        if (attribute == "currentSize") {
          new JLong(getCurrentSize())
        } else {
          if (attribute == "freeSize") {
            new JLong(getFreeSize())
          } else {
            null
          }
        }
      }
    }
  }

  override def setAttribute(attribute: Attribute): Unit = {}

    val att1Info = new MBeanAttributeInfo("sizeGB", "double", "Attribut", true, false, false)
    val att2Info = new MBeanAttributeInfo("entries", "long", "Attribut", true, false, false)
    val att3Info = new MBeanAttributeInfo("currentSize", "long", "Attribut", true, false, false)
    val att4Info = new MBeanAttributeInfo("freeSize", "long", "Attribut", true, false, false)
    val attribs = Array[MBeanAttributeInfo](att1Info, att2Info, att3Info, att4Info)

    val p1Info = new MBeanParameterInfo("sizeGB", "java.lang.Double", "New cache size")
    val params = Array[MBeanParameterInfo](p1Info)

    val op1Info = new MBeanOperationInfo(changeCacheSizeOperationName, "Change cache size",
                              params,
                              "Double", MBeanOperationInfo.ACTION)
    val ops     = Array[MBeanOperationInfo](op1Info)

    val info = new MBeanInfo(this.getClass.getName, "TestBean for Scray",
      attribs, null, ops, null)


  override def getMBeanInfo(): MBeanInfo = info

  override def setAttributes(attributes: AttributeList): AttributeList =
    getAttributes(attributes.asList.map(_.getName).toArray)

  override def getAttributes(attributes: Array[String]): AttributeList = {
    val results = new AttributeList
    attributes.foreach(name => results.add(getAttribute(name)))
    results
  }

  override def invoke(actionName: String, params: Array[Object], signature: Array[String]): Object = {
    def handleCreateKeyValueCache[K, V](kvcache: KeyValueCache[K, V], newsize: Double): KeyValueCache[K, V] = {
      logger.info(s"Replacing Cache ${kvcache.sourceDiscriminant} with old size ${kvcache.cachesizegb}GB with new size ${newsize}")
            new KeyValueCache(
                  kvcache.sourceDiscriminant,
                  kvcache.valueSerializer,
                  newsize,
                  kvcache.numberentries)

    }
    def completeCacheOperation[T](oldCache: Option[Cache[T]], newSize: Double): Unit = {
        oldCache match {
          case Some(ocache) => Registry.replaceCache(name, oldCache, ocache match {
            case kvcache: KeyValueCache[_, _] => handleCreateKeyValueCache(kvcache, newSize)
            case _ =>
              logger.error("Cache type not identifyable. Will not replace the cache!")
              ocache
          })
          case None => logger.error("Cache type not identifyable. Will not replace the cache!")
        }
    }
    actionName match {
      case `changeCacheSizeOperationName` =>
        val newsize: JDouble = params(0).asInstanceOf[JDouble]
        completeCacheOperation(Registry.getCache(name), newsize)
        "Your request has been approved"
      case _ => s"Action ${actionName} unknown"
    }
  }
}
