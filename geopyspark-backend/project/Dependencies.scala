import sbt._

object Dependencies {
  val geotrellis = "org.locationtech.geotrellis" %% "geotrellis-spark" % Version.geotrellis

  val sparkCore = "org.apache.spark"            %% "spark-core"       % "1.2.2"
}
