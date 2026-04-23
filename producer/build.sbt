name := "social-media-producer"
version := "1.0"
scalaVersion := "2.12.17"

libraryDependencies ++= Seq(
  "org.apache.kafka"  % "kafka-clients"       % "3.4.0",
  "com.typesafe.play" %% "play-json"           % "2.9.4",
  "ch.qos.logback"    % "logback-classic"      % "1.4.7"
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _                        => MergeStrategy.first
}
