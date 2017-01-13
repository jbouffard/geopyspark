import Dependencies._

name := "geotrellis-testkit"

resolvers ++= Seq(
  "Location Tech GeoTrellis Snapshots" at "https://repo.locationtech.org/content/repositories/geotrellis-snapshots",
  Resolver.mavenLocal
)

libraryDependencies ++= Seq(
  geotrellis,
  sparkCore % "provided"
)
