This repo contains the following files:


	new file:   LICENSE.txt
	new file:   README
	new file:   pom.xml
	new file:   src/main/java/TestSpark.java
	new file:   src/main/python/TestPythonApp.py
	new file:   src/main/scala/TestSparkApp.scala
	new file:   src/test/resources/test.csv

Each file represents an implementation of a Spark Application in Python, Java and Scala.

The objective of this task is to use Apache Spark:

http://spark.apache.org/docs/latest/quick-start.html
http://spark.apache.org/docs/latest/programming-guide.html

to implement 3 methods:

      def filter(self):
          # here filter out all rows that contain characters
          return None;

      def getAverage(self):
         # use spark sql to find the average of column a. The average should be added to the dataset as a new column
         return None;

      def getAverageByGroup(self):
         # use spark sql to find the average of column B Grouped by column A. The average should be added to the dataset as a new column
      
that will operate on the input file:
            
                      src/test/resources/test.csv

The code required should implement a solution to each of these 3 methods in Java OR Scala OR Python OR ALL of them to achieve the requirements above.

As it stands, the code does NOT fully compile, is NOT initialised with a correct spark context. Please fix the code to make it compile (in the language that you choose)

Whatever language you use, you *must* write unit tests in the src/test/ folder that verifies the filter, average and average by group use cases.
   
To get bonus points for Java and Scala applications, modify the `pom.xml` to produce an "uber jar". That is a Jar file with *all* dependancies include in the jar.
  
  