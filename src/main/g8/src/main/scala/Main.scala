package com.patelec.loadtable

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession._
import com.hortonworks.hwc.HiveWarehouseSession
import com.hortonworks.hwc.HiveWarehouseSession._
import org.apache.spark.sql.functions.col

object Main extends App {
  if (args.length == 0) {
    println("Two parameters required: db_name, table_name")
  }

  // getting parameters from arguments
  val db_name = args(0)
  val table_name = args(1)

  // spark session
  val spark =
    SparkSession.builder
      .appName("Spark LoadTable App")
      .config("hive.load.data.owner", "hive")
      .getOrCreate()
  // hive session with HWC
  val hive = com.hortonworks.spark.sql.hive.llap.HiveWarehouseBuilder
    .session(spark)
    .build()

  // data frame with proalpha table
  val df = spark.read
    .format("jdbc")
    .option(
      "url",
      "jdbc:datadirect:openedge://192.168.104.135:12001;databaseName=pavar"
    )
    .option("dbtable", "PUB." + table_name)
    .option("user", "sql_user_pi")
    .option("password", "pAdbPI!2017?")
    .option("driver", "com.ddtek.jdbc.openedge.OpenEdgeDriver")
    .load()

  // build sql query String to create table in Hive
  var insert_table_query = "create table " + table_name + " ("
  for (f <- df.schema.fields) {
    insert_table_query = insert_table_query.concat(
      " " + f.name + " " + f.dataType.simpleString + ","
    )
  }
  insert_table_query = insert_table_query.dropRight(1)
  insert_table_query = insert_table_query.concat(")")

  // set database and drop table if exists
  hive.setDatabase(db_name)
  hive.dropTable(table_name, true, true)

  // create table in hive
  hive.setDatabase(db_name)
  hive.executeUpdate(insert_table_query)

  // write table on csv
  // df.write
  //   .format("csv")
  //   .mode("overwrite")
  //   .save("/tmp/examples/csv")

  // println("Wrote on csv")

  // populate table with proalpha table
  df.write
    .format("com.hortonworks.spark.sql.hive.llap.HiveWarehouseConnector")
    .mode("overwrite")
    .option("table", table_name)
    .save()

  //closing session
  spark.close
  hive.close

}
