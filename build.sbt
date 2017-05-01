name := "sparktest"

version := "1.0"

scalaVersion := "2.11.8"

// spark version to be used
val sparkVersion = "2.1.0"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.databricks" %% "spark-csv" % "1.5.0"
)