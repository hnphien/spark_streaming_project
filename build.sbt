ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.5"

val ConfluentVersion = "7.0.0"

lazy val root = (project in file("."))
  .settings(
    name := "scala-spark",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.2",
    libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.1.2",
    libraryDependencies += "io.delta" %% "delta-core" % "1.0.0",
    libraryDependencies += "org.apache.spark" %% "spark-avro" % "3.1.2",
    resolvers += "Confluent".at("https://packages.confluent.io/maven/"),
    libraryDependencies += "org.apache.kafka" % "kafka-clients"                % "2.5.1",
    libraryDependencies += "io.confluent"     % "kafka-schema-registry-client" % ConfluentVersion,
    libraryDependencies += "io.confluent"     % "kafka-avro-serializer"        % ConfluentVersion,
    libraryDependencies +=  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.12.5",
    libraryDependencies += "io.github.agolovenko" %% "json-to-avro-converter" % "1.0.1",
    libraryDependencies += "org.apache.kafka" %% "kafka" % "3.0.0",
    libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.1.2",
    libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4",
    libraryDependencies += "io.spray" %% "spray-json" % "1.3.5",
    libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.1.2",
  )