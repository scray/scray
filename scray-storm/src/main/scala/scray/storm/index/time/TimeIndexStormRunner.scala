package scray.storm.index.time

import com.datastax.driver.core.ConsistencyLevel
import com.twitter.algebird.SetMonoid
import com.twitter.scalding.Args
import com.twitter.storehaus.algebra.Mergeable
import com.twitter.storehaus.cassandra.cql.CQLCassandraCollectionStore
import com.twitter.storehaus.cassandra.cql.CQLCassandraConfiguration
import com.twitter.storehaus.cassandra.cql.CassandraTupleStore
import com.twitter.summingbird.Options
import com.twitter.summingbird.Platform
import com.twitter.summingbird.TimeExtractor
import com.twitter.summingbird.batch.BatchID
import com.twitter.summingbird.batch.Batcher
import com.twitter.summingbird.online.MergeableStoreFactory
import com.twitter.summingbird.online.option.{FlatMapParallelism, SummerParallelism}
import com.twitter.summingbird.option.CacheSize
import com.twitter.summingbird.storm.{Storm, StormExecutionConfig}
import com.twitter.tormenta.spout.KafkaSpout
import com.twitter.util.Future
import com.websudos.phantom._
import java.io.DataInputStream
import scray.common.properties.ScrayProperties
import shapeless._
import shapeless.UnaryTCConstraint._
import shapeless.ops.hlist._
import scray.storm.scheme.GeneralJournalScheme
import com.twitter.tormenta.spout.Spout
import scray.storm.scheme.ScrayKafkaJournalEntry


class TimeIndexStormRunner[InKey <: String, InVal, Value, RefStreamType] (val streamer: TimeIndexStreamerOnline[InKey, InVal, Value, RefStreamType], 
        lookupFunction: RefStreamType => (InKey, InVal), columnFamily: CQLCassandraConfiguration.StoreColumnFamily, 
        batcher: Batcher, readReference: DataInputStream => RefStreamType)
            (implicit val timeextractor: TimeExtractor[ScrayKafkaJournalEntry[RefStreamType]], cp: CassandraPrimitive[Value]) {
  
  import scray.storm.index.time._
  // TODO: do property initialization right here
  
  object KafkaSpoutConfig {
    val APP_ID = "timeIndexKafkaSpout"
    val ZK_ROOT = "/kafka-spout"
    val BROKER_PATH = "/brokers"
    lazy val brokers = ScrayProperties.getPropertyValue("ZOOKEEPER_HOSTS")
    lazy val topic = ScrayProperties.getPropertyValue("KAFKA_TOPIC")
    lazy val spout = new KafkaSpout(new GeneralJournalScheme[RefStreamType](readReference), brokers, BROKER_PATH, topic, APP_ID, ZK_ROOT)
                           with Spout[ScrayKafkaJournalEntry[RefStreamType]]
  }
  
  def getStore(consistency: ConsistencyLevel): Storm#Store[(Tuple2[Int, Int], Tuple1[Long]), Set[Value]] = {
    // check that the store is there; if not we create it
    val mergeSemigroup = new SetMonoid[Value]
    //
    
    val store = new CQLCassandraCollectionStore[HListTypeRK, HListTypeCK, Set[Value], Value, SerializersTypeRK, SerializersTypeCK](
      columnFamily, serializersRK, columnNamesRK, serializersCK, columnNamesCK, valueColumnName, consistency)(mergeSemigroup)
//    val tupleStore = new CassandraTupleStore[TupleTypeRK, TupleTypeCK, Set[Value], HListTypeRK, HListTypeCK, SerializersTypeRK, SerializersTypeCK](
//            store, ((0,0), Tuple1(0l))) with MergeableStore[TupleTypeKey, Set[Value]]  {
//        override def semigroup = mergeSemigroup
//        override def merge(kv: TupleTypeKey): Future[Option[Set[Value]]] = {
//          Future.value(None)
//        }
//    }
    val tupleStore = new Mergeable[((Tuple2[Int, Int], Tuple1[Long]), BatchID), Set[Value]] {
      override def semigroup = mergeSemigroup
      override def merge(kv: (((Tuple2[Int, Int], Tuple1[Long]), BatchID), Set[Value])): Future[Option[Set[Value]]] = {
        Future.value(None)
      }
    }
    MergeableStoreFactory({ () => tupleStore }, batcher)
  }
  
  def apply(args: Args): StormExecutionConfig = {
    new StormExecutionConfig {
      override val name = "TimeIndexKafkaSpout"

      override def transformConfig(config: Map[String, AnyRef]): Map[String, AnyRef] = {
        config 
        // this is an option, if we re-create indices very often (i.e. disable msg-acking): 
        // ++ List((StormConfig.TOPOLOGY_ACKER_EXECUTORS -> (new java.lang.Integer(0))))
      }

      override def getNamedOptions: Map[String, Options] = Map(
        "DEFAULT" -> Options().set(SummerParallelism(3))
                      .set(FlatMapParallelism(80))
                      .set(CacheSize(100))
      )

      override def graph = streamer.indexing(KafkaSpoutConfig.spout, getStore(ScrayProperties.getPropertyValue("")))
    }
  }
  
}
