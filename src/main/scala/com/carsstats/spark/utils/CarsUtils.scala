package com.carsstats.spark.utils

import com.google.gson._

object CarsUtils {
  case class Cars (
                    lat : Double,
                    long : Double,
                    vehiculeId : String,
                    isFailing : Boolean,
                    temperature : Int,
                    engineTemperature : Int,
                    fuelInTank : Int,
                    isMoving : Boolean,
                    timestamp : Long
                  )


  def parseFromJson(lines:Iterator[String]):Iterator[Cars] = {
    val gson = new Gson
    lines.map(line => gson.fromJson(line, classOf[Cars]))
  }
}
