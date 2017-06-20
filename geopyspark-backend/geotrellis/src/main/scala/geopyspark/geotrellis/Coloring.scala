package geopyspark.geotrellis

import geotrellis.raster._
import geotrellis.raster.histogram._
import geotrellis.raster.render._

object Coloring {
  def getNamedRamp(name: String): ColorRamp = {
    name match {
      case "hot" => ColorRamps.HeatmapDarkRedToYellowWhite
      case "coolwarm" => ColorRamps.BlueToRed
      case "magma" => ColorRamps.Magma
      case "inferno" => ColorRamps.Inferno
      case "plasma" => ColorRamps.Plasma
      case "viridis" => ColorRamps.Viridis

      case "BlueToOrange" => ColorRamps.BlueToOrange
      case "LightYellowToOrange" => ColorRamps.LightYellowToOrange
      case "BlueToRed" => ColorRamps.BlueToRed
      case "GreenToRedOrange" => ColorRamps.GreenToRedOrange
      case "LightToDarkSunset" => ColorRamps.LightToDarkSunset
      case "LightToDarkGreen" => ColorRamps.LightToDarkGreen
      case "HeatmapYellowToRed" => ColorRamps.HeatmapYellowToRed
      case "HeatmapBlueToYellowToRedSpectrum" => ColorRamps.HeatmapBlueToYellowToRedSpectrum
      case "HeatmapDarkRedToYellowWhite" => ColorRamps.HeatmapDarkRedToYellowWhite
      case "HeatmapLightPurpleToDarkPurpleToWhite" => ColorRamps.HeatmapLightPurpleToDarkPurpleToWhite
      case "ClassificationBoldLandUse" => ColorRamps.ClassificationBoldLandUse
      case "ClassificationMutedTerrain" => ColorRamps.ClassificationMutedTerrain
      case "Magma" => ColorRamps.Magma
      case "Inferno" => ColorRamps.Inferno
      case "Plasma" => ColorRamps.Plasma
      case "Viridis" => ColorRamps.Viridis
    }
  }

  def makeColorMap(breaks: Array[Int], name: String): GTColorMap = GTColorMap(breaks, getNamedRamp(name))
  def makeColorMap(breaks: Array[Double], name: String): GTColorMap = GTColorMap(breaks, getNamedRamp(name))

  def makeColorMap(hist: Histogram[Int], name: String): GTColorMap = GTColorMap.fromQuantileBreaks(hist, getNamedRamp(name))
  def makeColorMap(hist: Histogram[Double], name: String)(implicit dummy: DummyImplicit): GTColorMap = GTColorMap.fromQuantileBreaks(hist, getNamedRamp(name))

}

object ColorRamp {
  def get(name: String): Array[Int] =
    Coloring.getNamedRamp(name).colors.toArray

  def get(name: String, numColors: Int): Array[Int] =
    Coloring.getNamedRamp(name).stops(numColors).colors.toArray

  def getHex(name: String): Array[String] =
    get(name).map(x => s"#${x.toHexString.toUpperCase}")

  def getHex(name: String, numColors: Int): Array[String] =
    get(name, numColors).map(x => s"#${x.toHexString.toUpperCase}")
}
