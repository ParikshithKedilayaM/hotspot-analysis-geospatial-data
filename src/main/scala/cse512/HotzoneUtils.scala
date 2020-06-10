package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // YOU NEED TO CHANGE THIS PART
    try {
      // queryRectangle format = (a,b,c,d), we need to check the point inside the rectangle boundry lines, double datatype eg. -155.940114(float makes 94011 so use double),19.081331,-155.618917,19.5307
     val rectanglePoints = queryRectangle.split(",")
      val x1 = rectanglePoints(0).toDouble
      val y1 = rectanglePoints(1).toDouble
      val x2 = rectanglePoints(2).toDouble
      val y2 = rectanglePoints(3).toDouble
      val points = pointString.split(",")
      val x = points(0).toDouble
      val y = points(1).toDouble
      if ((x >= Math.min(x1,x2) && x <= Math.max(x1,x2)) && (y >= Math.min(y1,y2) && y <= Math.max(y1,y2))){
        return true;
      }
      else{
        return false;
      }
    }
    catch {
      case _: Throwable => return false
    }
    
  }

  

}
