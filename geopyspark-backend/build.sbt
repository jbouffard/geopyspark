lazy val commonSettings = Seq(
  version := Version.geopyspark,
  scalaVersion := Version.scala,
  crossScalaVersions := Version.crossScala,
  description := "GeoPySpark Demo",
  organization := "org.locationtech.geotrellis",
  licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html")),
  scalacOptions ++= Seq(
    "-deprecation",
    "-unchecked",
    "-Yinline-warnings",
    "-language:implicitConversions",
    "-language:reflectiveCalls",
    "-language:higherKinds",
    "-language:postfixOps",
    "-language:existentials",
    "-feature"),

  shellPrompt := { s => Project.extract(s).currentProject.id + " > " }
)

lazy val root = Project("root", file(".")).
  aggregate(geotrellisProject,
    geotrellisTestkit,
    geotrellisTest).
  settings(commonSettings: _*)

lazy val geotrellisProject = Project("geotrellis-backend", file("geotrellis")).
  settings(commonSettings: _*)

lazy val geotrellisTestkit = Project("geotrellis-testkit", file("geotrellis-testkit")).
  dependsOn(geotrellisProject).
  settings(commonSettings: _*)

lazy val geotrellisTest = Project("geotrellis-test", file("geotrellis-test")).
  dependsOn(geotrellisProject, geotrellisTestkit).
  settings(commonSettings:_*)

/*
lazy val core = Project("core", file("core")).
  settings(commonSettings: _*)
*/
