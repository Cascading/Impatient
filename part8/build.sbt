// thus spake Twitter...
// this does not work

import AssemblyKeys._

assemblySettings

name := "impatient"

organization := "org.cascading"

version := "1.0"

scalaVersion := "2.9.2"

scalacOptions ++= Seq("-unchecked", "-deprecation")

resolvers += "Concurrent Maven Repo" at "http://conjars.org/repo"

// Use ScalaCheck
resolvers ++= Seq(
  "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
  "releases"  at "http://oss.sonatype.org/content/repositories/releases"
)

//resolvers += "Twitter Artifactory" at "http://artifactory.local.twitter.com/libs-releases-local"

libraryDependencies += "cascading" % "cascading-core" % "2.0.2"

libraryDependencies += "cascading" % "cascading-local" % "2.0.2"

libraryDependencies += "cascading" % "cascading-hadoop" % "2.0.2"

libraryDependencies += "cascading.kryo" % "cascading.kryo" % "0.4.5"

libraryDependencies += "com.twitter" % "maple" % "0.2.4"

libraryDependencies += "commons-lang" % "commons-lang" % "2.4"

libraryDependencies += "org.scala-tools.testing" % "specs_2.8.1" % "1.6.6" % "test"

libraryDependencies += "com.joestelmach" % "natty" % "0.7"

libraryDependencies += "io.backchat.jerkson" % "jerkson_2.9.2" % "0.7.0"

libraryDependencies ++= Seq(
  "org.scalacheck" %% "scalacheck" % "1.10.0" % "test",
  "org.scala-tools.testing" % "specs_2.9.0-1" % "1.6.8" % "test"
)
