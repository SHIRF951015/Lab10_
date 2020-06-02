import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
object Lab10 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CounterDemo").setMaster("local[*]")
    val sc = new SparkContext(conf);
    val spark = SparkSession.builder.appName("Test app").getOrCreate()
    val datafile = spark.read
      .format("com.databricks.spark.csv")
      .option("header",true)
      .load("C:\\Users\\Stone\\Desktop\\StudentsPerformance.csv")
    //  datafile.show()
    datafile.createOrReplaceTempView("students")
    //   spark.sql("SELECT COUNT(*) FROM students").show()
    spark.sql("select * from students").show()
    //spark.sql("select `race/ethnicity`, count(`race/ethnicity`) from students group by `race/ethnicity`").show()
    spark.sql("select gender,`race/ethnicity`, `parental level of education`, `math score` from students WHERE `math score` > 85").show()
    spark.sql("select gender,`race/ethnicity`, `parental level of education`, `math score` ,`writing score`,`reading score`from students WHERE `math score` > 75 and`reading score`>80 ").show()
    spark.stop()


  }

}