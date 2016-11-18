package scray.hesse.generator.hadoop

import java.util.ArrayList
import java.util.List
import scray.hesse.generator.GeneratorState
import scray.hesse.generator.HeaderInformation
import scray.hesse.hesseDSL.BodyStatement
import scray.hesse.hesseDSL.MaterializedViewStatement
import scray.hesse.hesseDSL.SupportedDBMSSystems

/**
 * generates a Hadoop job written in Scala that implements the requested Hesse script.
 * 
 * Properties of these jobs will be:
 * 	- Batch only
 *  - No versioning
 *  - fast Hadoop jobs
 * 
 * + On the pro-side of this type of jobs will be that they are able to use the
 *   optimized Hadoop stuff, like the CQLBulkColumnFamilyFormat for Cassandra, and do not
 *   rely on frameworks making these exceptionally fast writers.
 * - On the down-side these jobs are verbose and hard to edit. They do not provide
 *   versioning as well. 
 */
class HesseHadoopJobGenerator {
	
	/**
	 * creates import statements, class definition and 
	 */
	def generateFileBody(GeneratorState state, HeaderInformation header, MaterializedViewStatement view, List<BodyStatement> bodyStatements) {
		val mapperGenerator = new HesseHadoopMapperGenerator
		val reducerGenerator = new HesseHadoopReducerGenerator
		val writableGenerator = new HesseHadoopWritableGenerator
		
		'''
		// generated using Hesse!
		package «header.modelname»
		
		import org.apache.hadoop.conf.Configured
		«IF header.isDBMSUsed(#[view], SupportedDBMSSystems.CASSANDRA)»
		import com.datastax.driver.core._
		import org.apache.cassandra.hadoop.cql3.CqlInputFormat
		«ELSEIF header.isDBMSUsed(#[view], SupportedDBMSSystems.MYSQL) || header.isDBMSUsed(#[view], SupportedDBMSSystems.ORACLE)»
		import java.sql.{ PreparedStatement, ResultSet, Types }
		«ENDIF»
		import org.apache.hadoop.fs.Path
		import org.apache.hadoop.io._
		import org.apache.hadoop.mapred._
		import org.apache.hadoop.util.{ Tool, ToolRunner }
		import java.util.{Date, Iterator => JIterator, UUID, Set => JSet, Map => JMap}
		import scala.util.Try
		import scala.collection.mutable.LinkedHashMap
		import java.security.MessageDigest
		import java.util.zip.CRC32
		import java.util.zip.Checksum
		import java.math.{BigInteger => JBigInteger, BigDecimal => JBigDecimal}
		import scala.math.{BigInt, BigDecimal}
		
		«state.protectedRegions.protect("IMPORTS_MAIN_FILE", "// place additional imports here", false)»
		
		«mapperGenerator.generateMapper(state, header, view, bodyStatements)»
		
		«reducerGenerator.generateReducer(state, header, view, bodyStatements)»
		
		/**
		 * Runner code
		 */
		object «header.modelname + view.name»HadoopJob extends Configured with Tool {
			«state.protectedRegions.protect("MAIN_POST_PROCESSING", "// place protected post-processing code here", false)»
			
			«generateRunnerVars(state, header, view, bodyStatements)»
			
			«generateMain(state, header, view, bodyStatements)»
			
			«generateRun(state, header, view, bodyStatements)»
		}
		
		'''
	}
	
	/**
	 * creates the main method starting the ToolRunner class
	 */
	def generateMain(GeneratorState state, HeaderInformation header, MaterializedViewStatement view, List<BodyStatement> bodyStatements) {
		'''
		/**
		 * main method, executing ToolRunner
		 */
		def main(args: Array[String]) = {
			«IF header.isDBMSUsed(bodyStatements, SupportedDBMSSystems.CASSANDRA)»
			session.execute(s"CREATE KEYSPACE IF NOT EXISTS WITH replication = $replication;")
			«ENDIF»
			«state.protectedRegions.protect("MAIN_PRE_PROCESSING", "// place protected pre-processing code here", false)»
			val exitCode = ToolRunner.run(new JobConf(), «header.modelname + view.name»HadoopJob, args)
			«state.protectedRegions.protect("MAIN_POST_PROCESSING", "// place protected post-processing code here", false)»
			«IF header.isDBMSUsed(bodyStatements, SupportedDBMSSystems.CASSANDRA)»
			session.close
			«ENDIF»
			System.exit(exitCode)
		}
		'''
	}

	/**
	 * creates the main method starting the ToolRunner class
	 */
	def generateRun(GeneratorState state, HeaderInformation header, MaterializedViewStatement view, List<BodyStatement> bodyStatements) {
		'''
		/**
		 * run method, executed by ToolRunner
		 */
		override def run(args: Array[String]): Int = {
			val config = getConf().asInstanceOf[JobConf]
			val conf = new JobConf(config, «header.modelname + view.name»HadoopJob.getClass)
			conf.setOutputKeyClass(classOf[«header.modelname + view.name»KeyBytesWritable])
			conf.setOutputValueClass(classOf[«header.modelname + view.name»RowBytesWritable])
			«state.protectedRegions.protect("RUN_PRE_PROCESSING", "// place protected pre-processing code here", false)»
			val exitCode = ToolRunner.run(new JobConf(), «header.modelname + view.name»HadoopJob, args)
			JobClient.runJob(conf)
			«state.protectedRegions.protect("RUN_POST_PROCESSING", "// place protected post-processing and return code here
			0", false)»
		}
		'''
	}

	/**
	 * creates the main method starting the ToolRunner class
	 */
	def generateRunnerVars(GeneratorState state, HeaderInformation header, MaterializedViewStatement view, List<BodyStatement> bodyStatements) {
		'''
		«IF header.isDBMSUsed(#[view], SupportedDBMSSystems.CASSANDRA)»
		// TODO make these properties read from file
		val ip = "10.0.104.27"
		val replication = "{'class':'NetworkTopologyStrategy', 'DC1':3, 'DC2':3}"
		lazy val session = Cluster.builder().addContactPoint(ip).build().connect()
		«ENDIF»
		
		'''
	}
}
