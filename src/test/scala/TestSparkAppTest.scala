import TestSparkApp._
import org.apache.spark.sql.types.{StructField, StructType, IntegerType}
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

/**
  * Created by gfoote on 01/05/2017.
  *
  * Test suite for TestSparkApp class
  *
  */
class TestSparkAppTest extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll {

  val url = getClass.getResource("/test.csv")
  val testFilePath = url.toURI.getPath
  val session = startSession

  import session.implicits._

  var testObject: TestSparkApp = _

  override def beforeEach(): Unit = {
    session.sparkContext.setLogLevel("WARN")
    testObject = new TestSparkApp(session)
  }

  override protected def afterAll(): Unit = {
    session.stop()
  }

  /** Check file loading
    *
    */
  test("load file success") {
    val result: Dataset[String] = testObject.loadCsvFile(testFilePath).get
    val resultArray = result.collect()

    val expected = Array(
      "1,2,3",
      "4,2,4",
      "4,2,s",
      "4,?,3",
      "3,33,5",
      "2,3,3",
      "2,20,33")

    assert(resultArray === expected, " Load file success")
  }

  test("load file failure") {
    val result = testObject.loadCsvFile("not_vaild_file")

    assert(result.isFailure, "Load file fail")
  }

  /** Check filter function
    *
    */
  test("test filter 1") {
    val data: Dataset[String] = testObject.loadCsvFile(testFilePath).get
    val result = testObject.filter(data).collect()

    val expected = Array(
      Row(1, 2, 3),
      Row(4, 2, 4),
      Row(3, 33, 5),
      Row(2, 3, 3),
      Row(2, 20, 33)
    )
    assert(result === expected, "Filter test data 1")
  }

  test("test filter 2") {
    val data: Dataset[String] = Seq(
      "",
      ",,,",
      ",,").toDS
    val result = testObject.filter(data).collect()

    val expected = Array()

    assert(result === expected, "Filter test data 2")
  }

  test("test filter 3") {
    val data: Dataset[String] = Seq(
      "1,2,3",
      "1,2,3,4",
      "1","1,2").toDS
    val result = testObject.filter(data).collect()

    val expected = Array(
      Row(1, 2, 3)
    )
    assert(result === expected, "Filter test data 3")
  }

  test("test filter 4") {
    val data: Dataset[String] = Seq("1,2,3").toDS
    val result = testObject.filter(data).schema

    val fields = Seq(
      StructField("a", IntegerType, nullable = true),
      StructField("b", IntegerType, nullable = true),
      StructField("c", IntegerType, nullable = true)
    )
    val expected = StructType(fields)

    assert(result === expected, "Filter test data 4")
  }

  /** Check findaAverage function
    *
    */
  test("test findAverage") {
    val data = testObject.loadCsvFile(testFilePath).get
    val result = testObject.findAverage(data).collect()

    val expected = Array(
      Row(1, 2, 3, 2.4),
      Row(4, 2, 4, 2.4),
      Row(3, 33, 5, 2.4),
      Row(2, 3, 3, 2.4),
      Row(2, 20, 33, 2.4)
    )

    assert(result === expected, " findAverage")
  }

  /** Check findAveragePerGroup function
    *
    */
  test("testFindAveragePerGroup") {
    val data = testObject.loadCsvFile(testFilePath).get
    val result = testObject.findAveragePerGroup(data).collect()

    val expected = Array(
      Row(1, 2, 3, 2.0),
      Row(4, 2, 4, 2.0),
      Row(3, 33, 5, 33.0),
      Row(2, 3, 3, 11.5),
      Row(2, 20, 33, 11.5)
    )

    assert(result === expected, "findAveragePerGroup")
  }
}
