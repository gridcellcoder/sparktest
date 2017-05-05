
name := "sparktest"

version := "1.0"

scalaVersion := "2.11.8"

// spark version to be used
val sparkVersion = "2.1.0"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "com.databricks" %% "spark-csv" % "1.5.0"
)

// options for assemblt plugin
test in assembly := {}    // don't run tests before generating assembly
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)