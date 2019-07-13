name := "PubNative"

version := "0.1"
// Use scala version 2.12.0 in order to be able to use Spark
scalaVersion := "2.12.0"
libraryDependencies ++= Seq( 
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.9",
  "org.apache.spark" %% "spark-core" % "2.4.3",
  "org.apache.spark" %% "spark-sql" % "2.4.3"
)