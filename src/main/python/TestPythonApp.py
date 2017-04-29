# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
#     specific language governing permissions and limitations
# under the License.

from pyspark.sql import SparkSession


class TestPythonApp:
    def filter(self):
        # here filter out all rows that contain characters
        return None;

    def getAverage(self):
        # use spark sql to find the average of column a. The average should be added to the dataset as a new column
        return None;

    def getAverageByGroup(self):
        #  use spark sql to find the average of column B Grouped by column A. The average should be added to the dataset as a new column
        return None;

    def newSession(self):
        SparkSession \
            .builder \
            .appName("Python Spark SQL basic example") \
            .getOrCreate()
