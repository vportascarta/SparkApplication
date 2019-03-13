name := "TSPARK"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.scala-lang.modules" %% "scala-swing" % "2.0.3"
libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.11.12"


//libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.3.0" exclude("org.apache.hadoop", "hadoop-yarn-server-web-proxy")
//libraryDependencies += "org.apache.spark" % "spark-graphx_2.11" % "2.3.0" exclude("org.apache.hadoop", "hadoop-yarn-server-web-proxy")

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.4.0"


//mainClass in assembly := Some("ca.lif.sparklauncher.app.Application")
mainClass in assembly := Some("ca.lif.sparklauncher.console.MainConsole")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}