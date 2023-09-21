ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.14"

scalacOptions += "-deprecation"


lazy val root = (project in file("."))
  .settings(
    name := "ScalaProject",
      libraryDependencies += "com.google.cloud" % "google-cloud-storage" % "2.20.1",
      libraryDependencies += "com.google.cloud.spark" %% "spark-bigquery" % "0.29.0",
      libraryDependencies += "com.google.auth" % "google-auth-library-oauth2-http" % "1.16.0",
      libraryDependencies += "com.google.cloud" % "google-cloud-dataproc" % "4.9.0",
      libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.3",
      //libraryDependencies += "com.google.cloud.spark" %% "spark-bigquery-with-dependencies_2.12" % "0.23.0",//0.28
      libraryDependencies += "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % "0.29.0" % "provided",
      //libraryDependencies +="org.apache.spark" %% "spark-sql-kafka-0-10" % "3.1.3",
      libraryDependencies += "com.opencsv" % "opencsv" % "5.7.1",
      //libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.2.1",
      //libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.2.1",
      libraryDependencies += "com.google.cloud" % "google-cloud-pubsub" % "1.124.2",
      libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.1.3",
      //libraryDependencies +="org.jsoup" % "jsoup" % "1.14.3",
      libraryDependencies +="org.apache.spark" %% "spark-core" % "3.1.3",
      //libraryDependencies +="org.apache.beam" % "beam-sdks-java-io-google-cloud-platform" % "2.34.0",
      libraryDependencies += "org.scalatest" %% "scalatest" % "3.3.0-SNAP4" % Test,
      libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.17.0" % Test,
      libraryDependencies += "org.scala-lang" % "scala-library" % "2.12.14",
      libraryDependencies += "com.google.api-client" % "google-api-client" % "1.32.1",
      libraryDependencies += "org.apache.bahir" %% "spark-streaming-pubsub" % "2.4.0",
      libraryDependencies += "com.github.mrpowers" %% "spark-daria" % "1.2.3"

  )





