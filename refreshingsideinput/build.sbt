import sbt.Keys._
import sbt._

lazy val commonSettings = Defaults.coreDefaultSettings ++ Seq(
  organization := "com.mrcalonso",
  // Semantic versioning http://semver.org/
  version := "0.1.0-SNAPSHOT",
  scalaVersion := "2.12.4",
  scalacOptions ++= Seq("-target:jvm-1.8",
    "-deprecation",
    "-feature",
    "-unchecked"),
  javacOptions ++= Seq("-source", "1.8",
    "-target", "1.8")
)
lazy val paradiseDependency =
  "org.scalamacros" % "paradise" % scalaMacrosVersion cross CrossVersion.full
lazy val macroSettings = Seq(
  libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  addCompilerPlugin(paradiseDependency)
)
lazy val noPublishSettings = Seq(
  publish := {},
  publishLocal := {},
  publishArtifact := false
)
lazy val root: Project = Project(
  "refreshingsideinput",
  file(".")
).settings(
  commonSettings ++ macroSettings ++ noPublishSettings,
  description := "RefreshingSideInput",
  libraryDependencies ++= Seq(
    "com.spotify" %% "scio-core" % scioVersion,
    "com.spotify" %% "scio-test" % scioVersion % "test",
    "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
    // optional dataflow runner
    "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion,
    "org.slf4j" % "slf4j-simple" % "1.7.25"
  )
).enablePlugins(PackPlugin)
val scioVersion = "0.5.4"
val beamVersion = "2.4.0"
val scalaMacrosVersion = "2.1.1"

