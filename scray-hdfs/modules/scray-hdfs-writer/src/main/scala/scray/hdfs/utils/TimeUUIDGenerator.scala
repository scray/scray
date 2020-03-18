package scray.hdfs.utils

import java.util.UUID
import java.net.NetworkInterface
import java.net.InetAddress

class TimeUUIDGenerator {


	var macAddress = getMac;
	var lastTime: Long = 0;

 var clockSequence = 0L;

    def generateIdFromTimestamp(currentTimeMillis: Long): UUID = {
		var time: Long = 0



		time = currentTimeMillis;

		// low Time
		time = currentTimeMillis << 32;

		// mid Time
		time |= ((currentTimeMillis & 0xFFFF00000000L) >> 16);

		// hi Time
		time |= 0x1000 | ((currentTimeMillis >> 48) & 0x0FFF); // version 1

		var clock_seq_hi_and_reserved = clockSequence;

		clock_seq_hi_and_reserved <<= 48;

		var cls = clock_seq_hi_and_reserved;

		var lsb = cls | macAddress;

			return new UUID(time, lsb)

    } 
    
    	def getMac: Long = {
		var  macAddressAsLong = 0L;
			val address = InetAddress.getLocalHost();
			val ni = NetworkInterface.getByInetAddress(address);
			if (ni != null) {
				//byte[] mac = ni.getHardwareAddress(); // availabe since Java 1.6
				val mac = "01:23:45:67:89:ab".getBytes();
				//Converts array of unsigned bytes to an long
				
				for(i <- 0 to (mac.length -1)) {
					macAddressAsLong <<= 8;
					macAddressAsLong =  (macAddressAsLong ^ (mac(i) & 0xFF)).asInstanceOf[Long]
				}
			}
		return macAddressAsLong;
}
}