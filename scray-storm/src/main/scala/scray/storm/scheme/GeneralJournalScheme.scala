package scray.storm.scheme

import com.twitter.tormenta.scheme.Scheme
import java.io.ByteArrayInputStream
import java.io.DataInputStream

/**
 * scheme to read a stream of references from Kafka
 */
class GeneralJournalScheme[T](readReference: DataInputStream => T) extends Scheme[ScrayKafkaJournalEntry[T]] {

   override def decode(bytes: Array[Byte]): TraversableOnce[ScrayKafkaJournalEntry[T]] = {
     val bais = new ByteArrayInputStream(bytes)
     val dis = new DataInputStream(bais)
     try {
       Seq(ScrayKafkaJournalEntry(dis.readUTF(), readReference(dis), dis.readLong(), dis.readBoolean()))
     } finally {
       dis.close()
     }
   } 
  
}