name := "agg"
version := "1.0"
scalaVersion := "2.11.12"

mainClass in (Compile, packageBin) := Some("com.sberbankde.agg")

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.5"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.5" % "provided"