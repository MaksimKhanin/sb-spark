name := "filter"
version := "1.0"
scalaVersion := "2.11.12"

assemblyJarName in assembly := "filter_2.11-1.0.jar"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.5"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5"

libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.5" % "provided"