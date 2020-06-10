package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  pickupInfo.createOrReplaceTempView("pickupInfo")


  //Selecting the hot cells and their counts(of pickups)
  var hotCellPoints = spark.sql("select x, y, z, count(*) as pickupCount from pickupInfo where x>="
+ minX +" and x<=" + maxX + " and y>=" + minY +" and y<= " + maxY+ " and z>=" + minZ+" and z<="
+maxZ  + " group by z, y, x")
  hotCellPoints.createOrReplaceTempView("hotCellPoints")

  // Computing Sum of Xj and XjSquare from hotCellPoints
  var Xj_XjSquare = spark.sql("select sum(pickupCount) as sumOfXj, sum(pickupCount * pickupCount) as sqsumOfXj from hotCellPoints")
  Xj_XjSquare.createOrReplaceTempView("Xj_XjSquare")

  //fetching values for Xj and XjSquare
  var Xj_XjSquare_row = Xj_XjSquare.first()
  val sumOfXj = Xj_XjSquare_row.getLong(0).toDouble
  val sqsumOfXj = Xj_XjSquare_row.getLong(1).toDouble

  // Computing mean and S needed for the Getis-Ord Stat score
  val X_mean = (sumOfXj / numCells.toDouble)
  val S = math.sqrt((sqsumOfXj / numCells.toDouble) - (X_mean.toDouble * X_mean.toDouble))

  // Self join on hotCellPoint to get neighbours of each cell
  spark.udf.register("neighbors", (inputX: Int, inputY: Int, inputZ: Int, minX: Int, minY: Int, minZ: Int, maxX: Int, maxY: Int, maxZ: Int) => HotcellUtils.calculateNeighbors(inputX, inputY, inputZ, minX, minY, minZ, maxX, maxY, maxZ))
  var neighborCells = spark.sql("select neighbors(hcp1.x, hcp1.y, hcp1.z, " +
    +minX+ "," + minY + "," + minZ + ","  + maxX + "," + maxY +"," + maxZ+ ") as neighborCellCount, hcp1.x " +
    "as x, hcp1.y as y, hcp1.z as z, sum(hcp2.pickupCount) as WijXj" +
    " from hotCellPoints as hcp1, hotCellPoints as hcp2  " +
    "where (hcp2.x = hcp1.x-1 or hcp2.x = hcp1.x or hcp2.x = hcp1.x+1) " +
    "and (hcp2.y = hcp1.y-1 or hcp2.y = hcp1.y or hcp2.y = hcp1.y+1)" +
    " and (hcp2.z = hcp1.z-1 or hcp2.z = hcp1.z or hcp2.z = hcp1.z+1)" +
    " group by hcp1.z, hcp1.y, hcp1.x")
  neighborCells.createOrReplaceTempView("neighborCells")

  spark.udf.register("GetisOrdStat", (WijXj: Int, neighborCellCount: Int, numCells: Int, X_mean: Double, S: Double) => HotcellUtils.calculateGsordScore(WijXj, neighborCellCount, numCells, X_mean, S))

  var GetisOrdStatCells = spark.sql(s"""select GetisOrdStat(WijXj, neighborCellCount, $numCells, $X_mean, $S) as GetisOrdStat, x, y, z from neighborCells order by GetisOrdStat desc""")
  GetisOrdStatCells.createOrReplaceTempView("GetisOrdStatCells")

  // x,y,z values sorted based on GetisOrdStat score
  var resultInfo = spark.sql("select x, y, z  from GetisOrdStatCells")
  resultInfo.createOrReplaceTempView("finalPickupInfo")

  return resultInfo
}
}
