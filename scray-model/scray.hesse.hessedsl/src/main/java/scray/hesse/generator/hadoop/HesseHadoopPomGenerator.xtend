package scray.hesse.generator.hadoop

import java.util.Calendar
import java.util.List
import scray.hesse.generator.GeneratorState
import scray.hesse.generator.HeaderInformation
import scray.hesse.hesseDSL.BodyStatement
import scray.hesse.hesseDSL.MaterializedViewStatement
import scray.hesse.hesseDSL.SupportedDBMSSystems

/**
 * generates project pom.xml
 */
class HesseHadoopPomGenerator {
	
	def String generatePOM(GeneratorState state, HeaderInformation header, MaterializedViewStatement view, List<BodyStatement> bodyStatements) {
		'''
		<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
			xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/maven-v4_0_0.xsd">
			<modelVersion>4.0.0</modelVersion>
			<groupId>«header.modelname»</groupId>
			<artifactId>«header.modelname + view.name»</artifactId>
			<version>1.0-SNAPSHOT</version>
			<name>${project.artifactId}</name>
			<description>«header.modelname + view.name» is a Hadoop job, created using Hesse</description>
			<inceptionYear>«Calendar.getInstance().get(Calendar.YEAR)»</inceptionYear>
		
			<properties>
				<maven.compiler.source>1.6</maven.compiler.source>
				<maven.compiler.target>1.6</maven.compiler.target>
				<encoding>UTF-8</encoding>
				<scala.version>2.10.4</scala.version>
				<scala.compat.version>2.10</scala.compat.version>
			</properties>
		
			<dependencies>
				«IF header.isDBMSUsed(#[view], SupportedDBMSSystems.CASSANDRA)»
				<dependency>
					<groupId>com.datastax.cassandra</groupId>
					<artifactId>cassandra-driver-core</artifactId>
					<version>2.1.5</version>
				</dependency>
				<dependency>
					<groupId>org.apache.cassandra</groupId>
					<artifactId>cassandra-all</artifactId>
					<version>2.1.3</version>
				</dependency>
				«ENDIF»
				<dependency>
					<groupId>org.scala-lang</groupId>
					<artifactId>scala-library</artifactId>
					<version>${scala.version}</version>
				</dependency>
				<dependency>
					<groupId>org.apache.hadoop</groupId>
					<artifactId>hadoop-core</artifactId>
					<version>1.2.1</version>
					<scope>provided</scope>
				</dependency>
				<dependency>
					<groupId>com.twitter</groupId>
					<artifactId>util-core_2.10</artifactId>
					<version>6.22.0</version>
				</dependency>
				<dependency>
					<groupId>joda-time</groupId>
					<artifactId>joda-time</artifactId>
					<version>2.9.1</version>
				</dependency>
				
				«state.protectedRegions.protect("POM_DEPS", "<!-- place additional dependencies here -->", true)»
			</dependencies>
		
			<build>
				<sourceDirectory>src/main/scala</sourceDirectory>
				<testSourceDirectory>src/test/scala</testSourceDirectory>
				<plugins>
					<plugin>
						<!-- see http://davidb.github.com/scala-maven-plugin -->
						<groupId>net.alchim31.maven</groupId>
						<artifactId>scala-maven-plugin</artifactId>
						<version>3.2.0</version>
						<executions>
							<execution>
								<goals>
									<goal>compile</goal>
									<goal>testCompile</goal>
								</goals>
								<configuration>
									<args>
										<arg>-dependencyfile</arg>
										<arg>${project.build.directory}/.scala_dependencies</arg>
									</args>
								</configuration>
							</execution>
						</executions>
					</plugin>
					<plugin>
						<groupId>org.apache.maven.plugins</groupId>
						<artifactId>maven-surefire-plugin</artifactId>
						<version>2.18.1</version>
						<configuration>
							<useFile>false</useFile>
							<disableXmlReport>true</disableXmlReport>
							<!-- If you have classpath issue like NoDefClassError,... -->
							<!-- useManifestOnlyJar>false</useManifestOnlyJar -->
							<includes>
								<include>**/*Test.*</include>
								<include>**/*Suite.*</include>
							</includes>
						</configuration>
					</plugin>
					<plugin>
						<artifactId>maven-assembly-plugin</artifactId>
						<version>2.4</version>
						<executions>
							<execution>
								<id>distro-assembly</id>
								<phase>package</phase>
								<goals>
									<goal>single</goal>
								</goals>
								<configuration>
									<descriptors>
										<descriptor>assembly.xml</descriptor>
									</descriptors>
								</configuration>
							</execution>
						</executions>
					</plugin>
					«state.protectedRegions.protect("POM_PLUGINS", "<!-- place additional plugins here -->", true)»
				</plugins>
			</build>
		
			<repositories>
				<repository>
					<id>twttr</id>
					<url>http://maven.twttr.com/</url>
				</repository>
				<repository>
					<id>conjars.org</id>
					<url>http://conjars.org/repo</url>
				</repository>
				<repository>
					<id>clojars.org</id>
					<url>http://clojars.org/repo</url>
				</repository>
				«state.protectedRegions.protect("POM_REPS", "<!-- place additional repositories here -->", true)»
			</repositories>
		
			<dependencyManagement>
				<dependencies>
					<dependency>
						<groupId>com.google.code.findbugs</groupId>
						<artifactId>jsr305</artifactId>
						<version>1.3.9</version>
					</dependency>
					<dependency>
						<groupId>org.apache.thrift</groupId>
						<artifactId>libthrift</artifactId>
						<version>0.9.2</version>
					</dependency>
					<dependency>
						<groupId>com.datastax.cassandra</groupId>
						<artifactId>cassandra-driver-core</artifactId>
						<version>2.1.5</version>
						<classifier>shaded</classifier>
						<exclusions>
							<exclusion>
								<groupId>io.netty</groupId>
								<artifactId>*</artifactId>
							</exclusion>
						</exclusions>
					</dependency>
					«state.protectedRegions.protect("POM_DEPMAN", "<!-- place additional managed dependencies here -->", true)»
				</dependencies>
			</dependencyManagement>
		</project>
		'''
	}
}