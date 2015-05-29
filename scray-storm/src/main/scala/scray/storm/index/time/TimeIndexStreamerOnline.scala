package scray.storm.index.time

import com.twitter.summingbird.batch.Batcher
import scray.storm.index.time.copied.TimeIndexStreamer
import scray.storm.index.time.copied.TimeIndex
import scray.storm.index.time.copied.IndexMember
import com.twitter.summingbird.storm.Storm
import com.twitter.summingbird.Producer
import scray.storm.scheme.ScrayKafkaJournalEntry

/**
 * Online extensions of TimeIndexStreamer.
 * 
 * Specializes randomSpread to 1 as nothing more is really needed.
 * 
 */
abstract class TimeIndexStreamerOnline[InKey <: String, InVal, Value, RefStreamType](val metainfo: TimeIndex, 
        val onlineBatcher: Batcher, lookupFunction: ScrayKafkaJournalEntry[RefStreamType] => (InKey, InVal)) 
    extends TimeIndexStreamer[InKey, InVal, Value] with Serializable {

  override def meta: IndexMember = metainfo
  
  override implicit lazy val batcher = onlineBatcher

  override def randomSpread: Int = 1
  
  def indexing(
    source: Producer[Storm, ScrayKafkaJournalEntry[RefStreamType]],
    store: Storm#Store[(Tuple2[Int, Int], Tuple1[Long]), Set[Value]]) = {
      super.indexing[Storm](source.map { lookupFunction }, store)
  }
}