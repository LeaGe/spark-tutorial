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

        //table.show()

        val headers = table.columns.map(name => List(name))

        val cells = table.flatMap(row => row.toSeq.asInstanceOf[Seq[String]].zip(headers))
        //cells.show()

        val temp = cells.rdd.reduceByKey((a, b) => (a ++ b).distinct)
        //temp.toDF().show(false)

        temp
      })
      .reduce((a, b) => a.union(b))
      .reduceByKey((a, b) => (a ++ b).distinct)
      .map(_._2) //same as (a => a._2)
      .flatMap(attributeSet => {
        val allColumns = attributeSet
        attributeSet.map(column => (column,allColumns.filterNot(_==column))) //a => a==column
      })
      .reduceByKey((a,b) => a.intersect(b))
      .filter(line => line._2 != List.empty)

      .toDF().show(false)
  }
}
