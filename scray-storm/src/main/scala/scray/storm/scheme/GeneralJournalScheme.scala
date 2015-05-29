package scray.storm.scheme

import com.twitter.tormenta.scheme.Scheme
import java.io.ByteArrayInputStream
import java.io.DataInputStream
import scala.annotation.tailrec

/**
 * scheme to read a stream of references from Kafka
 */
class GeneralJournalScheme[T](readReference: DataInputStream => T) extends Scheme[ScrayKafkaJournalEntry[T]] {

  @tailrec def readFullBuffer(bais: ByteArrayInputStream, dis: DataInputStream, 
                              acc: Seq[ScrayKafkaJournalEntry[T]]): Seq[ScrayKafkaJournalEntry[T]] = {
    if(bais.available() <= 0) {
      acc
    } else {
      readFullBuffer(bais, dis, acc ++ 
                     Seq(ScrayKafkaJournalEntry(dis.readUTF(), readReference(dis), dis.readLong(), dis.readBoolean())))
    }
  } 
  
  override def decode(bytes: Array[Byte]): TraversableOnce[ScrayKafkaJournalEntry[T]] = {
    val bais = new ByteArrayInputStream(bytes)
    val dis = new DataInputStream(bais)
    try {
      readFullBuffer(bais, dis, Seq())
    } finally {
      dis.close()
    }
  }
}