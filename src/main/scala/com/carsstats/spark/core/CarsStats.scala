package com.carsstats.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import com.carsstats.spark.utils._
import com.carsstats.spark.utils.CarsUtils.Cars


object CarsStats {

  val pathToFileCars = "data/cars-info.json"

  /**
    *  Load the data from the json file and return an RDD of Cars
    */
  def loadDataCars(): RDD[Cars] = {
    // create spark configuration and spark context
    val conf = new SparkConf()
      .setAppName("CarsStats")
      .setMaster ("local[*]")

    val sc = SparkContext.getOrCreate(conf)

    sc.setLogLevel("OFF")

    // Load the data and parse it into Cars info.
    // Look at the Cars info Object in the CarsUtils class.
    sc.textFile(pathToFileCars)
      .mapPartitions(CarsUtils.parseFromJson(_))

  }

  def city(cars : Cars): String = {
    case class City (
                      latMin : Double,
                      latMax : Double,
                      longMin : Double,
                      longMax : Double,
                      name : String
                    )
    val paris = City(48.816170,48.902004,2.231161,2.414551,"Paris")
    val sanFrancisco = City(37.708284,37.832052,-122.515191,-122.358991,"San Francisco")

    val cities = List(paris,sanFrancisco)

    cities
      .filter(city => city.latMin <= cars.lat && city.latMax >= cars.lat && city.longMin <= cars.long && city.longMax >= cars.long)
      .head
      .name
  }

  def failingTempAvg(): Int  = {
    val lol = loadDataCars()
      .filter(cars => cars.isFailing)
      .map(cars => (cars.engineTemperature,1))
      .reduce((x,y) => (x._1 + y._1,x._2 + y._2))

    lol._1/lol._2
  }

  def movingTempAvg(): Int  = {
    val lol = loadDataCars()
    .filter(cars => cars.isMoving)
    .map(cars => (cars.engineTemperature,1))
    .reduce((x,y) => (x._1 + y._1,x._2 + y._2))

    lol._1/lol._2
  }

  def failingCities(): RDD[(String,Int)] = {
    loadDataCars()
      .filter(cars => cars.isFailing)
      .map(cars => (city(cars),1))
      .reduceByKey(_ + _)
      .sortBy(-_._2)
  }

  def outOfFuelFailingPercentage(): Double = {
    val outOfFuelFails = loadDataCars()
      .filter(cars => cars.isFailing && cars.fuelInTank == 0).count()

    outOfFuelFails.toDouble/loadDataCars().count().toDouble * 100
  }

  def main(args: Array[String]): Unit = {
    println("Average temperature of failing cars engines = " + failingTempAvg() + "°C")
    println("Avergage temperature of moving cars engines = " + movingTempAvg() + "°C")
    print("Cities where cars failed and the number of occurence = ")
    println(failingCities().collect().foreach(println))
    println("Percentage of cars which broke down running out of fuel = " + outOfFuelFailingPercentage() + "%")
  }
}
