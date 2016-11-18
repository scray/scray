package scray.storm.index.time.copied

import com.twitter.summingbird.Platform
import com.twitter.summingbird.Producer
import com.twitter.summingbird.Producer.toKeyed
import scray.storm.index.time.copied._

/**
 * Main 'logic' behind time indexing jobs.
 *
 * @author christian
 *
 * @param <K>
 * @param <V>
 */
abstract class TimeIndexStreamer[InKey <: String, InVal, Value] extends Serializable {
  // set specific job meta information
  def meta: IndexMember

  // Set default batcher for this type of time indexing job
  //implicit lazy val batcher = {IndexPropertiesRegistration.configure(None, "Client-MT-Index-Property-Config"); TIME_INDEX_BATCHER() }
  implicit lazy val batcher = TIME_INDEX_BATCHER()

  // How to extract the time from input values
  def indexedTimeExtractor(value: (InKey, InVal)): Option[Long]

  // How to generate ValueT instances from InKey(<:String) instances
  def toValue(id: String): Value

  def yearExtractor(time: Long): Int = {
    val cal = new java.util.GregorianCalendar();
    cal.setTime(new java.util.Date(time));
    cal.get(java.util.Calendar.YEAR);
  }

  def randomSpread: Int = scala.util.Random.nextInt(INDEX_ROW_SPREAD) + 1

  def indexing[P <: Platform[P]](
    source: Producer[P, (InKey, InVal)],
    store: P#Store[(Tuple2[Int, Int], Tuple1[Long]), Set[Value]]) = {
    source
      // get rid of empty options
      .filter { indexedTimeExtractor(_).nonEmpty }
      // pull out id and indexed field value
      .map { rec => (rec._1, indexedTimeExtractor(rec).get) }
      // construct the index tuples
      .map { case (id, time) => (((yearExtractor(time), randomSpread), Tuple1(time)), Set(toValue(id))) }
      // sum up
      .sumByKey(store)
      .name(meta.identifier)
  }
}
