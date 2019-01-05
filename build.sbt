name := """DOAN3"""
organization := "com.doan3"

version := "1.0-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.12.6"




libraryDependencies += jdbc
libraryDependencies += guice
libraryDependencies += "org.scalatestplus.play" %% "scalatestplus-play" % "3.1.2" % Test
libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.41"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.2"
libraryDependencies += "com.google.guava" % "guava" % "27.0-jre"

//libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"
//libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.0" % "runtime"

libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.7"
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.7"
libraryDependencies += "net.liftweb" %% "lift-json" % "3.3.0"

// JSON.
libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4"

libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "2.5.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"
libraryDependencies += "org.mongodb.spark" %% "mongo-spark-connector" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"

// Adds additional packages into Twirl
//TwirlKeys.templateImports += "com.doan3.controllers._"

// Adds additional packages into conf/routes
// play.sbt.routes.RoutesKeys.routesImport += "com.doan3.binders._"
