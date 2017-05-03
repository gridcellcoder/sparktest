import java.io.InputStream
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}
import TestSparkApp._


/**
  * Created by gfoote on 01/05/2017.
  */
class TestSparkAppTest extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll {

  override def beforeEach() {

  }

  override def afterEach() {

  }

  override protected def beforeAll(): Unit = {

    val session = startSession

    val path = getClass.getResource("/test.csv").getPath
    val testDS = session.sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .load(path).as[String]

  }

  override protected def afterAll(): Unit = {}

  test("testFilter") {

  }

  test("testFindAverage") {

  }

  test("testStartSession") {

  }

  test("testFindAveragePerGroup") {

  }

}
