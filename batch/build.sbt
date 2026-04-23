name := "social-media-batch"
version := "1.0"
scalaVersion := "2.12.17"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"           % "3.4.0" % "provided",
  "org.apache.spark" %% "spark-sql"            % "3.4.0" % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.4.0",
  "ch.qos.logback"    % "logback-classic"      % "1.4.7"
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case PathList("reference.conf") => MergeStrategy.concat
  case _                          => MergeStrategy.first
}
