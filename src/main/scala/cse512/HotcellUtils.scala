package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  // YOU NEED TO CHANGE THIS PART


  def calculateGsordScore(totalPickupPoint: Int, totalNeighbors: Int, numCells: Int, mean: Double, S: Double): Double =
  {
    var numerator = totalPickupPoint.toDouble - (mean * totalNeighbors.toDouble)
    var denominator = S * math.sqrt(((numCells.toDouble * totalNeighbors.toDouble) - math.pow(totalNeighbors.toDouble, 2)) / (numCells.toDouble - 1.0))
    var result = (numerator/denominator).toDouble

  return result
  }

  def calculateNeighbors (inputX: Int, inputY: Int, inputZ: Int, minX: Int, minY: Int, minZ: Int, maxX: Int, maxY: Int, maxZ: Int): Int =
    {
      //if input matches any min/max increment counter to get number of surrounding neighbors(max being 26 as given in description)
      var neighbour_check = 0
      var total_neighbours = 26
      var missing_neighbours = 0
      if (inputX == minX || inputX == maxX){
        neighbour_check += 1
      }
      if (inputY == minY || inputY == maxY){
        neighbour_check += 1
      }
      if (inputZ == minZ || inputZ == maxZ){
        neighbour_check += 1
      }

      missing_neighbours =   neighbour_check match {
        case 0 =>   0
        case 1 =>   9
        case 2 =>   15
        case 3 =>   19

      }
      return total_neighbours - missing_neighbours
    }

}
