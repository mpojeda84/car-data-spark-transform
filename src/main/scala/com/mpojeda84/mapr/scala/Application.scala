package com.mpojeda84.mapr.scala


import com.mapr.db.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

///user/mapr/tables/car-data-transformed
object Application {

  def main (args: Array[String]): Unit = {

    val argsConfiguration = Configuration.parse(args)

    val spark = SparkSession.builder.appName("Car Data Transformation").getOrCreate

    println("### RUNNING ###")
    println(argsConfiguration.source)
    println(argsConfiguration.target)
    println(argsConfiguration.community)

    val df = spark.loadFromMapRDB(argsConfiguration.source)

    df.persist(StorageLevel.MEMORY_ONLY)

    df.createOrReplaceTempView("raw_data")


    val ndf_vinmakeyear = spark.sql("SELECT VIN AS `vin`, Make AS `make`, `Year` AS `year` FROM raw_data GROUP BY vin, make, year")
    val highestSpeedToday = spark.sql("SELECT DISTINCT `VIN` AS `vin`, max(cast(`speed` AS Double)) AS `highestSpeedToday` FROM raw_data GROUP BY `VIN`, CAST(`hrtimestamp` AS DATE)")
    val highestSpeedThisWeek = spark.sql("SELECT `VIN` AS `vin`, max(cast(`speed` AS Double)) AS `highestSpeedThisWeek` FROM raw_data GROUP BY `vin`")
    val avgSpeed = spark.sql("SELECT `VIN` AS `vin`, avg(cast(`speed` AS Double)) AS `avgSpeed` FROM raw_data GROUP BY `vin`")
    val highestFuelEconomy = spark.sql("SELECT DISTINCT `VIN` AS `vin`, max(cast(`instantFuelEconomy` AS Double)) AS `bestFuelEconomy` FROM raw_data GROUP BY `VIN`")
    val totalFuelEconomy = spark.sql("SELECT DISTINCT `VIN` AS `vin`, avg(cast(`instantFuelEconomy` AS Double)) AS `totalFuelEconomy` FROM raw_data GROUP BY `VIN`")

    val df2 = ndf_vinmakeyear.join(highestSpeedToday, "vin").join(highestSpeedThisWeek, "vin").join(avgSpeed, "vin").join(highestFuelEconomy, "vin").join(totalFuelEconomy, "vin")
    df2.withColumn("_id", col("vin")).saveToMapRDB(argsConfiguration.target)


    val  commAvgSpeed = spark.sql("SELECT avg(`speed`) AS `avgCommunitySpeed` FROM raw_data")
    commAvgSpeed.withColumn("_id", lit("speed")).saveToMapRDB(argsConfiguration.community)

    println("### FINISHED ###")
    spark.stop()


  }



}