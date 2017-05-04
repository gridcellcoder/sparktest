import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, Row}
import TestSparkApp._


/**
  * Created by gfoote on 01/05/2017.
  */
class TestSparkAppTest extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll {

  // Test data
  val url = getClass.getResource("/test.csv")
  val testFilePath = url.toURI.getPath
  val session = startSession

  var testObject: TestSparkApp = _

  import session.implicits._



  override def beforeEach(): Unit = {
    session.sparkContext.setLogLevel("WARN")
    testObject = new TestSparkApp(session)
  }

  override def afterEach() {
  }

  override protected def beforeAll(): Unit = {
  }

  override protected def afterAll(): Unit = {
    session.stop()
  }

  test("Load file") {
    val result: Dataset[String] = testObject.loadFile(testFilePath)

    val resultArray = result.collect()

    val expected = Array(
      "1,2,3",
      "4,2,4",
      "4,2,s",
      "4,?,3",
      "3,33,5",
      "2,3,3",
      "2,20,33")

    assert(resultArray === expected," Load file")
  }


  test("test filter") {

    val data: Dataset[String] = testObject.loadFile(testFilePath)

    val result = testObject.filter(data).collect()

    val expected = Array(
      Row(1,2,3),
      Row(4,2,4),
      Row(3,33,5),
      Row(2,3,3),
      Row(2,20,33)
    )
    assert(result === expected, "Filter test data")
  }

  test("test findAverage") {

    val data = testObject.loadFile(testFilePath)

    val result = testObject.findAverage(data).collect()

    val expected = Array(
      Row(1,2,3,2.4),
      Row(4,2,4,2.4),
      Row(3,33,5, 2.4),
      Row(2,3,3, 2.4),
      Row(2,20,33, 2.4)
      )

    assert(result === expected," findAverage")
  }

  test("testStartSession") {
    fail("Not implemented")
  }

  test("testFindAveragePerGroup") {
    fail("Not implemented")
  }

}
