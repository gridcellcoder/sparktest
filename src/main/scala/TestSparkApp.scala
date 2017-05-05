import org.apache.spark
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._


import scala.util.Try

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

class TestSparkApp(session: SparkSession) {

  import session.implicits._


  def loadFile(path: String): Dataset[String] = {
    session.sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load(path)
      .map(r => r.mkString(","))
  }

  def filter(frame: Dataset[String]): DataFrame = {
    //here filter out all rows that contain characters

    // actually check that each row consists of numbers seperated by commas
    // since other functions rely on this

    frame.flatMap(r => {
      val fields = r.split(",")
      val numbers = fields.flatMap( s => Try(s.toDouble).toOption)
      numbers match {
        case Array(x,y,z) => Some((x,y,z))
        case _ => None}
    }).toDF("a","b","c")
  }

  def findAverage(frame: Dataset[String]): DataFrame = {
    //use spark sql to find the average of column A. The average should be added to the dataset as a new column
    val df = filter(frame)
    val average = df.agg(avg($"a")).first.getDouble(0)
    df.withColumn("avg(a)", lit(average) )
  }

  def findAveragePerGroup(frame: Dataset[String]): Dataset[String] = {
    //use spark sql to find the average of column B Grouped by column A. The average should be added to the dataset as a new column

    val test = session.sparkContext.parallelize(List("test"))
    session.createDataset[String](test)
  }
}

object TestSparkApp {
  def startSession(): SparkSession = {
    SparkSession.builder
      .master("local")
      .appName("Scala Spark SQL basic example")
      .getOrCreate
  }
}


