name := "Test"
organization := "CS236"
version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.0"
libraryDependencies += "org.apache.spark" % "spark-catalyst_2.11" % "2.1.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.0"
libraryDependencies += "com.vividsolutions" % "jts-core" % "1.14.0"
libraryDependencies += "InitialDLab" % "simba_2.11" % "1.0"

mainClass in (Compile, run) := Some("Test")
mainClass in (Compile, packageBin) := Some("Test")