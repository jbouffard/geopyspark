import Dependencies._

name := "geotrellis-backend"

resolvers ++= Seq(
  "Location Tech GeoTrellis Snapshots" at "https://repo.locationtech.org/content/repositories/geotrellis-snapshots",
  Resolver.mavenLocal
)

libraryDependencies ++= Seq(
  sparkCore % "provided",
  geotrellis
)
