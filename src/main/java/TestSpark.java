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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class TestSpark {

    public static void main(String[] args) {

    }

    private Dataset filter(Dataset<String> frame) {
        //here filter out all rows that contain characters
    }

    private Dataset findAverage(Dataset<String> frame) {
        //use spark sql to find the average of column a. The average should be added to the dataset as a new column
    }


    private Dataset findAveragePerGroup(Dataset<String> frame) {
        //use spark sql to find the average of column B Grouped by column A. The average should be added to the dataset as a new column
    }

    private void startSession() {
        return new SparkSession()
                .builder()
                .appName("Java Spark SQL basic example")
                .getOrCreate();
    }
}
