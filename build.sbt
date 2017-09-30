lazy val commonSettings = Seq(

version := "0.0.1",
scalaVersion := "2.11.8",
EclipseKeys.withSource := true,
parallelExecution in test := false,
test in assembly := {},
assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}
) ++ packAutoSettings

lazy val project = Project(
id = "exam", 
base = file(".")).settings(commonSettings).settings(
name := "exam",

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.0.2",
 "org.apache.spark" % "spark-sql_2.11" % "2.0.2",
  "org.apache.spark" % "spark-graphx_2.11" %"2.0.2",
  "org.apache.spark" % "spark-mllib_2.11" %"2.0.2"
)
)
