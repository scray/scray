package scray.hesse.generator.hadoop

import java.util.List
import scray.hesse.generator.GeneratorState
import scray.hesse.generator.HeaderInformation
import scray.hesse.generator.HessePlaformGenerator
import scray.hesse.hesseDSL.BodyStatement
import scray.hesse.hesseDSL.MaterializedViewStatement

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
class HesseHadoopGenerator implements HessePlaformGenerator {

	/**
	 * method that delegates generation of the target files to helper classes
	 * Creates maven-based project structure of the target project
	 */	
	override doGenerate(GeneratorState state, HeaderInformation header, MaterializedViewStatement view, List<BodyStatement> bodyStatements) {
		// generate project pom.xml
		val pomgenerator = new HesseHadoopPomGenerator
		val pomfilename = header.modelname + "/pom.xml"
		state.protectedRegions.readProtectedRegionsFromFile(pomfilename, state, true)
		state.fsa.generateFile(pomfilename, pomgenerator.generatePOM(state, header, view, bodyStatements))		

		// generate assembly.xml
		val assemblygenerator = new HesseHadoopAssemblyGenerator
		val asmfilename = header.modelname + "/assembly.xml"
		state.protectedRegions.readProtectedRegionsFromFile(asmfilename, state, true)
		state.fsa.generateFile(asmfilename, assemblygenerator.generateAssembly(state, header, view, bodyStatements))		

		// generate job file
		val jobgenerator = new HesseHadoopJobGenerator
		val jobfilename = header.modelname + "/src/main/scala/" + header.modelname + "/" + header.modelname + view.name + "HadoopJob.scala"
		state.protectedRegions.readProtectedRegionsFromFile(jobfilename, state, false)
		state.fsa.generateFile(jobfilename, jobgenerator.generateFileBody(state, header, view, bodyStatements))
	}

}
