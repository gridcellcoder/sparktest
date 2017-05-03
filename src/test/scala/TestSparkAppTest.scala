import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}
import org.apache.spark.sql.{Dataset, SparkSession}
import TestSparkApp._


/**
  * Created by gfoote on 01/05/2017.
  */
class TestSparkAppTest extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll {

  // Test data
  val url = getClass.getResource("/test.csv")
  val path = url.toURI.getPath
  val session = startSession

  import session.implicits._

  val testDS = session.sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .load(path)
    .as[InputData]


  override def beforeEach() {
  }

  override def afterEach() {
  }

  override protected def beforeAll(): Unit = {
  }

  override protected def afterAll(): Unit = {}

  test("Check test data") {
    assert(testDS.count() === 3, "Data not loaded")
  }

  test("testFilter") {

  }

  test("testFindAverage") {

  }

  test("testStartSession") {

  }

  test("testFindAveragePerGroup") {

  }

}
