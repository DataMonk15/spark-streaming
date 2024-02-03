name := "Spark"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.12.18"

// Add dependencies for spark compatibility with scala 2.13

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.1.2",
  "org.apache.spark" %% "spark-sql" % "3.1.2"
)

// Add streaming and mllib dependencies

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming" % "3.1.2",
  "org.apache.spark" %% "spark-mllib" % "3.1.2"
)


