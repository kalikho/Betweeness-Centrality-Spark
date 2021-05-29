import org.apache.logging.log4j.core.config.composite.MergeStrategy
import sun.security.tools.PathList

name := "Betweeness-Centrality-Spark"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.5"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5"

resolvers += "SparkPackages" at "https://dl.bintray.com/spark-packages/maven"
resolvers +=  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies += "graphframes" % "graphframes" % "0.7.0-spark2.4-s_2.11"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.4.5"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

