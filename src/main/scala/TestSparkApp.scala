import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

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

/** Class to demonstrate basic Spark functions.
  *
  * This is implmented as class wrapping a SparkSession object. There is a companion object that provides a helper
  * method that instantiates a local spark session.
  *
  * @param session Spark session to use for analysis
  */
class TestSparkApp(session: SparkSession) {

  import session.implicits._

  /** Load a csv file into a Dataset.
    *
    * A dataframe is loaded from the provided file path. The file is loaded as a CSV file, and then converted to a
    * dataset of Strings in CSV format. This provides some basic checking that the file is in a suitable format, and
    * provides the data in a format that fits the required API.
    *
    * @param path the path to the file to load
    * @return a Dataset of String, where strings are in CSV format
    */
  def loadFile(path: String): Dataset[String] = {
    session.sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load(path)
      .map(r => r.mkString(","))
  }

  /** Filters a Dataset of strings into Dataframe with three integer columns
    *
    * The filter function attempts to parse each string in the input data set into three integers. Strings that cannot
    * be successfully parsed are ignored. This ensures that the returned dataframe is suitable for subsequent analysis.
    *
    * The format of the numbers is not specified. Ints are used here because other functions group on these columns,
    * and grouping on continuous values (e.g. Double) makes no sense.
    *
    * @param frame the Dataset to filter
    * @return Dataframe with three columns [a; Int, b: Int, c: Int]
    */
  def filter(frame: Dataset[String]): DataFrame = {

    frame.flatMap(r => {
      // Try to convert each line into Array of Ints
      val numbers = r.split(",").flatMap(s => Try(s.toInt).toOption)
      // Check arrays have three values
      numbers match {
        case Array(x, y, z) => Some((x, y, z))
        case _ => None
      }
    }).toDF("a", "b", "c")
  }

  /** Find the average of the column derived from the first value in the csv strings in the dataset
    *
    * This function uses the filter function to convert Strings "a, b, c" to [a: Int, b: Int, c: Int]
    *
    * @param frame the dataset to analyse. This should contain strings of three integers in CSV format.
    * @return A dataframe containing the original data and a column containing the average of column 'a' appended
    */
  def findAverage(frame: Dataset[String]): DataFrame = {
    //use spark sql to find the average of column A. The average should be added to the dataset as a new column
    val df = filter(frame)
    val average = df.agg(avg($"a")).first.getDouble(0)
    df.withColumn("avg(a)", lit(average))
  }

  /** Find average of column 'b' grouped by column 'a'.
    *
    * @param frame the dataset to analyse. This should contain strings of three integers in CSV format.
    * @return A dataframe containing the original data and a column containing the average of column 'b' grouped by
    *         column 'a' appended
    */
  def findAveragePerGroup(frame: Dataset[String]): DataFrame = {
    //use spark sql to find the average of column B Grouped by column A. The average should be added to the dataset as a new column

    val df = filter(frame)
    val grouped = df.groupBy($"a").agg(avg($"b"))

    df.join(grouped, Seq("a"), "inner")
  }
}

/** Companion object for class.
  *
  */
object TestSparkApp {
  /** Gets or creates a local Spark Session
    *
    * @return a local Spark Session
    */
  def startSession(): SparkSession = {
    SparkSession.builder
      .master("local")
      .appName("GridCell Spark Test")
      .getOrCreate
  }
}


