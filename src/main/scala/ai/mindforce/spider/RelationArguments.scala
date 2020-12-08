package ai.mindforce.spider

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import scopt.OptionParser

object RelationArguments {

  def debugLoop(): Unit = {
    while (true) {
      Thread.sleep(10000000)
    }
  }

  def parse(program: (RelationConfig, SparkSession) => Unit, args: Array[String]): Unit = {
    val builder = SparkSession.builder()
      .appName("Shelob")

    val argOpt = parser().parse(args, (SparkConfig(builder), RelationConfig()))
    argOpt match {
      case Some((sparkConfig, relationConfig)) => runProgram(program, sparkConfig, relationConfig)
      case None => System.exit(-1)
    }
  }

  private def runProgram(program: (RelationConfig, SparkSession) => Unit,
                         sparkConfig: SparkConfig,
                         relationConfig: RelationConfig): Unit = {
    val spark = sparkConfig.builder.getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel(sparkConfig.logLevel)

    program(relationConfig, spark)
  }

  private def parser(): OptionParser[(SparkConfig, RelationConfig)] = {
    new OptionParser[(SparkConfig, RelationConfig)]("shelob") {
      head("Shelob MindForce©", "1.0")

      opt[String]('p', "parent-column").action {
        case (x, (s, c)) => (s, c.copy(parentCol = x))
      }
        .text("Name of the column with parent id")
      opt[String]('c', "child-column").action {
        case (x, (s, c)) =>
          (s, c.copy(childCol = x))
      }.text("Name of the column with child id")
      opt[String]('r', "relation-column").action {
        case (x, (s, c)) => (s, c.copy(relationCol = x))
      }.text("Name of the column with relation id")
      opt[String]('l', "log-level").action {
        case (x, (s, c)) => (s.copy(logLevel = x), c)
      }.text("Logging level of whole application")

      opt[String]('m', "master").action {
        case (x, (s, c)) => (s.copy(builder = s.builder.master(x)), c)
      }.text("Spark application master")
      opt[String]('n', "name").action {
        case (x, (s, c)) => (s.copy(builder = s.builder.appName(x)), c)
      }.text("Spark application name")
      opt[Unit]('h', "hive-support").action {
        case (_, (s, c)) => (s.copy(builder = s.builder.enableHiveSupport()), c)
      }.text("Enabling hive support")

      opt[Unit]("debug").hidden().action { case (_, (s, c)) => (s, c.copy(afterSave = debugLoop)) }
        .text("Turn on indefinite loop at the end of program. For debug purposes only. " +
          "BEWARE: The program never stops unless killed")

      cmd("csv").action { case (_, (s, c)) => (s, c.copy(dataSource = CSVDataSource())) }.
        text("Load data from CSV file").
        children(
          opt[String]('p', "path").action {
            case (x, (s, c)) =>
              (s, c.copy(dataSource = c.dataSource.asInstanceOf[CSVDataSource].copy(path = x)))
          }.text("Path to CSV file").required(),
          opt[String]("target-name").abbr("tgt").action {
            case (x, (s, c)) =>
              (s, c.copy(dataSource = c.dataSource.asInstanceOf[CSVDataSource].copy(result = x)))
          }.text("Path to result folder with saved csv").required()
        )

      cmd("parquet").action { case (_, (s, c)) => (s, c.copy(dataSource = ParquetDataSource())) }.
        text("Load data from parquet file").
        children(
          opt[String]('p', "path").action {
            case (x, (s, c)) =>
              (s, c.copy(dataSource = c.dataSource.asInstanceOf[ParquetDataSource].copy(path = x)))
          }.text("Path to folder with parquet files").required(),
          opt[String]("target-name").abbr("tgt").action {
            case (x, (s, c)) =>
              (s, c.copy(dataSource = c.dataSource.asInstanceOf[ParquetDataSource].copy(result = x)))
          }.text("Path to result folder with saved parquets").required()

        )

      cmd("database").action { case (_, (s, c)) => (s, c.copy(dataSource = TableDataSource())) }.
        text("Load data from hadoop").
        children(
          opt[String]('d', "database-name").action {
            case (x, (s, c)) =>
              (s, c.copy(dataSource = c.dataSource.asInstanceOf[TableDataSource].copy(database = x)))
          }.text("Name of database").required(),
          opt[String]('t', "table-name").action {
            case (x, (s, c)) =>
              (s, c.copy(dataSource = c.dataSource.asInstanceOf[TableDataSource].copy(relations = x)))
          }.text("Name of table").required(),
          opt[String]("target-name").abbr("tgt").action {
            case (x, (s, c)) =>
              (s, c.copy(dataSource = c.dataSource.asInstanceOf[TableDataSource].copy(result = x)))
          }.text("Name of result table").required()
        )

      help("help").text("Program help and usage")

      checkConfig {
        case (_, c) => if (c.dataSource == null) {
          failure("Choose some command for loading option")
        } else {
          success
        }
      }
    }
  }

  trait DataSource {
    def load(spark: SparkSession): DataFrame

    def save(dataFrame: DataFrame, saveMode: SaveMode): Unit
  }

  case class SparkConfig(
                          builder: SparkSession.Builder,
                          logLevel: String = "WARN"
                        )

  case class RelationConfig(
                             dataSource: DataSource = null,
                             parentCol: String = "party_id",
                             childCol: String = "child_id",
                             relationCol: String = "relations_value",
                             saveMode: SaveMode = SaveMode.Append,
                             afterSave: () => Unit = () => ()
                           )

  case class CSVDataSource(path: String = null, result: String = null)
    extends DataSource {

    val optionMap = Map(
      "delimiter" -> ",",
      "header" -> "true",
      "inferSchema" -> "true"
    )

    override def load(spark: SparkSession): DataFrame = {
      spark.read
        .format("csv")
        .options(optionMap)
        .load(path)
    }

    override def save(dataFrame: DataFrame, saveMode: SaveMode): Unit = {
      dataFrame
        .coalesce(1)
        .write
        .mode(saveMode)
        .format("csv")
        .options(optionMap)
        .save(result)
    }
  }

  case class ParquetDataSource(path: String = null, result: String = null)
    extends DataSource {
    override def load(spark: SparkSession): DataFrame = {
      spark.read
        .format("parquet")
        .load(path)
    }

    override def save(dataFrame: DataFrame, saveMode: SaveMode): Unit = {
      dataFrame
        .write
        .format("parquet")
        .mode(saveMode)
    }
  }

  case class TableDataSource(database: String = null, relations: String = null, result: String = null)
    extends DataSource {
    override def load(spark: SparkSession): DataFrame = {
      spark.catalog.refreshTable(s"$database.$relations")

      spark sql
        s"""
           |select
           | *
           |from $database.$relations
        """.stripMargin
    }

    override def save(dataFrame: DataFrame, saveMode: SaveMode): Unit = {
      dataFrame.write
        .mode(saveMode)
        .saveAsTable(s"$database.$relations")
    }
  }


}
