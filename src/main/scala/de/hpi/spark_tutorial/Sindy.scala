package de.hpi.spark_tutorial

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {

    // Importing implicit encoders for standard library classes and tuples that are used as Dataset types
    import spark.implicits._

    inputs
      .map(input => {
        val table = spark.read
          .option("header", "true")
          .option("delimiter", ";")
          .option("inferSchema", "false")
          .csv(input)

        val headers = table.columns.map(name => List(name))
        val cells = table.flatMap(row => row.toSeq.asInstanceOf[Seq[String]].zip(headers))
        cells.rdd.reduceByKey((a, b) => (a ++ b).distinct)
      })
      .reduce((a, b) => a.union(b))
      .reduceByKey((a, b) => (a ++ b).distinct)
      .map(_._2) //same as (a => a._2)
      .flatMap(attributeSet => {
        attributeSet.map(column => (column, attributeSet.filterNot(_==column))) //a => a==column
      })
      .reduceByKey((a,b) => a.intersect(b))
      .flatMap(a => a._2.map(b => (a._1, b)))
      .aggregateByKey(List().asInstanceOf[List[String]])(_ :+ _, _ ++ _)
      .sortByKey(numPartitions = 1)
      .map(a => s"${a._1} < ${a._2.mkString(", ")}")
      .collect()
      .foreach(println(_))
  }
}
