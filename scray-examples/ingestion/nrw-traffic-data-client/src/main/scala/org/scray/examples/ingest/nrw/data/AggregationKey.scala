package org.scray.examples.ingest.nrw.data

/**
 * TODO: adapt to key class which is used to group aggregates for your job
 */
case class AggregationKey(access: String, typ: String, category: String, direction: String) extends Ordered[AggregationKey] {
  import scala.math.Ordering._
  override def compare(that: AggregationKey): Int =
    Tuple4[String, String, String, String].compare((this.access, this.typ, this.category, this.direction),
        (that.access, that.typ, that.category, that.direction))
}

