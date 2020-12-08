package ai.mindforce.spider

import ai.mindforce.spider.RelationArguments.{CSVDataSource, RelationConfig}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{FlatSpec, Matchers}

class RelationSpec extends FlatSpec with Matchers {

  val spark: SparkSession = SparkSession.builder()
    .appName("Spider - Tests")
    .master("local[*]").getOrCreate()

  val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("OFF")

  val defConfig = RelationConfig()

  val pathPrefix = "./src/test/resources"

  "Relation calculation" should "compute and terminates on single node" in {
    val expected = Set(
      RelationResult(1L, 2L, 0.5)
    )

    val result = convertFrameToResult(
      Relation.calculateRelationFrame(
        defConfig.copy(dataSource = CSVDataSource(s"$pathPrefix/relations/single.csv")),
        spark
      )
    )

    result should equal(expected)
  }

  it should "compute multiply factor correctly" in {
    val expected = Set(
      RelationResult(1L, 2L, 0.5),
      RelationResult(2L, 3L, 0.5),
      RelationResult(1L, 3L, 0.25)
    )

    val result = convertFrameToResult(
      Relation.calculateRelationFrame(
        defConfig.copy(dataSource = CSVDataSource(s"$pathPrefix/relations/multiply.csv")),
        spark
      )
    )

    result should equal(expected)
  }

  it should "compute add factor correctly" in {
    val expected = Set(
      RelationResult(1L, 2L, 0.5),
      RelationResult(2L, 3L, 0.5),
      RelationResult(1L, 3L, 0.75)
    )

    val result = convertFrameToResult(
      Relation.calculateRelationFrame(
        defConfig.copy(dataSource = CSVDataSource(s"$pathPrefix/relations/add.csv")),
        spark
      )
    )

    result should equal(expected)
  }

  it should "stop during circle" in {
    val expected = Set(
      RelationResult(4L, 1L, 0.5),
      RelationResult(4L, 2L, 0.5),
      RelationResult(4L, 3L, 0.5),
      RelationResult(1L, 2L, 1.0),
      RelationResult(1L, 3L, 1.0),
      RelationResult(2L, 3L, 1.0),
      RelationResult(2L, 1L, 0.5),
      RelationResult(3L, 1L, 0.5),
      RelationResult(3L, 2L, 0.5)
    )

    val result = convertFrameToResult(
      Relation.calculateRelationFrame(
        defConfig.copy(dataSource = CSVDataSource(s"$pathPrefix/relations/circle.csv")),
        spark
      )
    )

    result should equal(expected)
  }

  it should "count triangle add with head" in {
    val expected = Set(
      RelationResult(1L, 2L, 0.05),
      RelationResult(1L, 4L, 0.05),
      RelationResult(1L, 3L, 0.05),
      RelationResult(2L, 3L, 1.0),
      RelationResult(2L, 4L, 1.0),
      RelationResult(4L, 3L, 0.3)
    )

    val result = convertFrameToResult(
      Relation.calculateRelationFrame(
        defConfig.copy(dataSource = CSVDataSource(s"$pathPrefix/relations/triangle.csv")),
        spark
      )
    )

    result should equal(expected)
  }

  it should "solve complex structure hiding owner ship with cycle" in {
    val expected = Set(
      RelationResult(1, 2, 0.05),
      RelationResult(1, 3, 0.05),
      RelationResult(1, 4, 0.05),
      RelationResult(1, 5, 0.05),
      RelationResult(2, 3, 1.0),
      RelationResult(2, 4, 1.0),
      RelationResult(2, 5, 1.0),
      RelationResult(3, 2, 0.85),
      RelationResult(3, 4, 0.85),
      RelationResult(3, 5, 0.85),
      RelationResult(4, 2, 0.1),
      RelationResult(4, 3, 0.1),
      RelationResult(4, 5, 1.0),
      RelationResult(5, 2, 0.1),
      RelationResult(5, 3, 0.1),
      RelationResult(5, 4, 0.1)
    )

    val result = convertFrameToResult(
      Relation.calculateRelationFrame(
        defConfig.copy(dataSource = CSVDataSource(s"$pathPrefix/relations/complex.csv")),
        spark
      )
    )

    result should equal(expected)
  }

  it should "solve two complex structures separately with same result" in {
    val expected = Set(
      RelationResult(1, 2, 0.05),
      RelationResult(1, 3, 0.05),
      RelationResult(1, 4, 0.05),
      RelationResult(1, 5, 0.05),
      RelationResult(2, 3, 1.0),
      RelationResult(2, 4, 1.0),
      RelationResult(2, 5, 1.0),
      RelationResult(3, 2, 0.85),
      RelationResult(3, 4, 0.85),
      RelationResult(3, 5, 0.85),
      RelationResult(4, 2, 0.1),
      RelationResult(4, 3, 0.1),
      RelationResult(4, 5, 1.0),
      RelationResult(5, 2, 0.1),
      RelationResult(5, 3, 0.1),
      RelationResult(5, 4, 0.1),

      RelationResult(10, 20, 0.05),
      RelationResult(10, 30, 0.05),
      RelationResult(10, 40, 0.05),
      RelationResult(10, 50, 0.05),
      RelationResult(20, 30, 1.0),
      RelationResult(20, 40, 1.0),
      RelationResult(20, 50, 1.0),
      RelationResult(30, 20, 0.85),
      RelationResult(30, 40, 0.85),
      RelationResult(30, 50, 0.85),
      RelationResult(40, 20, 0.1),
      RelationResult(40, 30, 0.1),
      RelationResult(40, 50, 1.0),
      RelationResult(50, 20, 0.1),
      RelationResult(50, 30, 0.1),
      RelationResult(50, 40, 0.1)
    )

    val result = convertFrameToResult(
      Relation.calculateRelationFrame(
        defConfig.copy(dataSource = CSVDataSource(s"$pathPrefix/relations/separate-complex.csv")),
        spark
      )
    )

    result should equal(expected)
  }


  it should "solve deep complex structure hiding owner ship with multiple cycles" in {
    val expected = Set(
      RelationResult(1,2,0.05),
      RelationResult(1,3,0.05),
      RelationResult(1,4,0.05),
      RelationResult(1,5,0.05),
      RelationResult(1,6,0.05),
      RelationResult(2,3,1.0),
      RelationResult(2,4,1.0),
      RelationResult(2,5,1.0),
      RelationResult(2,6,1.0),
      RelationResult(3,2,0.85),
      RelationResult(3,4,0.85),
      RelationResult(3,5,0.85),
      RelationResult(3,6,0.85),
      RelationResult(4,2,0.1),
      RelationResult(4,3,0.1),
      RelationResult(4,5,1.0),
      RelationResult(4,6,1.0),
      RelationResult(5,2,0.1),
      RelationResult(5,3,0.1),
      RelationResult(5,4,0.1),
      RelationResult(5,6,0.37)
    )

    val result = convertFrameToResult(
      Relation.calculateRelationFrame(
        defConfig.copy(dataSource = CSVDataSource(s"$pathPrefix/relations/super-complex.csv")),
        spark
      )
    )

    result should equal(expected)
  }

  def convertFrameToResult(dataFrame: DataFrame): Set[RelationResult] = dataFrame
    .collect()
    .map(r => RelationResult(r.getAs[Long](defConfig.parentCol), r.getAs[Long](defConfig.childCol),
      BigDecimal(r.getAs[Double](defConfig.relationCol)).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble))
    .toSet

  case class RelationResult(sourceId: Long, targetId: Long, relation: Double)

}
