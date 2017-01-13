import Dependencies._

name := "geotrellis-test"

libraryDependencies ++= Seq(
  sparkCore % "provided",
  geotrellis
)

assemblyMergeStrategy in assembly := {
  case "referejce.conf" => MergeStrategy.concat
  case "application.conf" => MergeStrategy.concat
  case "META-INF/MANIFEST.MF" => MergeStrategy.discard
  case "META-INF\\MANIFEST.MF" => MergeStrategy.discard
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.discard
  case "META-INF/ECLIPSEF.SF" => MergeStrategy.discard
  case _ => MergeStrategy.first
}
