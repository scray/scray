package scray.hesse.generator.hadoop

import java.util.List
import scray.hesse.generator.GeneratorState
import scray.hesse.generator.HeaderInformation
import scray.hesse.hesseDSL.BodyStatement
import scray.hesse.hesseDSL.MaterializedViewStatement

/**
 * generates an assembly file which is used by the build process to package the job-jar
 */
class HesseHadoopAssemblyGenerator {
	
	def generateAssembly(GeneratorState state, HeaderInformation header, MaterializedViewStatement view, List<BodyStatement> bodyStatements) {
		'''
		<assembly xmlns="http://maven.apache.org/plugins/maven-assembly-plugin/assembly/1.1.0"
			xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
			xsi:schemaLocation="http://maven.apache.org/plugins/maven-assembly-plugin/assembly/1.1.0 http://maven.apache.org/xsd/assembly-1.1.0.xsd">
		<id>job-distribution</id>

		  <formats>
		    <format>jar</format>
		  </formats>
		
		  <includeBaseDirectory>false</includeBaseDirectory>
		
		  <dependencySets>
		    <dependencySet>
		      <unpack>true</unpack>
		      <scope>runtime</scope>
		      <outputDirectory>/</outputDirectory>
		      <excludes>
		        <exclude>org.apache.hadoop:hadoop-core</exclude>
		        <exclude>${artifact.groupId}:${artifact.artifactId}</exclude>
		      </excludes>
		    </dependencySet>
		    <dependencySet>
		      <unpack>false</unpack>
		      <scope>system</scope>
		      <outputDirectory>lib</outputDirectory>
		      <excludes>
		        <exclude>${artifact.groupId}:${artifact.artifactId}</exclude>
		      </excludes>
		    </dependencySet>
		  </dependencySets>
		
		  <fileSets>
		    <fileSet>
		      <directory>${basedir}/target/classes</directory>
		      <outputDirectory>/</outputDirectory>
		      <excludes>
		        <exclude>*.jar</exclude>
		      </excludes>
		    </fileSet>
		  </fileSets>
		«state.protectedRegions.protect("ASM_INSTS", "<!-- place additional assembly instructions here -->", true)»
		</assembly>
		'''
	}
}