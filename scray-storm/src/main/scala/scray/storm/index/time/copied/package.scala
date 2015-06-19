package scray.storm.index.time

import com.twitter.summingbird.batch.Batcher
import scray.common.properties.predefined.PredefinedProperties

package object copied {
    val TIME_INDEX_BATCHER: () => Batcher = () => Batcher.ofDays(1)
    val INDEX_ROW_SPREAD = 1
}