name := "stackoverflow-analysis"

version := "0.1"

scalaVersion := "2.11.12"


libraryDependencies += "com.johnsnowlabs.nlp" %% "spark-nlp" % "2.2.1"
//libraryDependencies += "com.johnsnowlabs.nlp" %% "spark-nlp" % "2.2.1"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.3"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"

