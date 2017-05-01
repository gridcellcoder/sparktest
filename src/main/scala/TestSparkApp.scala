import org.apache.spark.sql.{Dataset, SparkSession}

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

class TestSparkApp {
  def filter(frame: Dataset[String]): Dataset = {
    //here filter out all rows that contain characters
  }

  def findAverage(frame: Dataset[String]): Dataset = {
    //use spark sql to find the average of column A. The average should be added to the dataset as a new column
  }

  def findAveragePerGroup(frame: Dataset[String]): Dataset = {
    //use spark sql to find the average of column B Grouped by column A. The average should be added to the dataset as a new column
  }

  def startSession() = {
    SparkSession.builder
      .master("local")
      .appName("Scala Spark SQL basic example")
      .getOrCreate
  }
}


