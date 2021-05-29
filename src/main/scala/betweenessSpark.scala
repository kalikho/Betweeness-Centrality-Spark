//Package name
package com.betweenessSpark

//Dependencies
import java.io.Serializable

import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame
import org.apache.spark.sql.types.{StringType, StructField, StructType}


trait Setter{
  val spark:SparkSession = SparkSession.builder
    .appName("Osint Project")
    .getOrCreate()




  @transient implicit var sc:SparkContext = spark.sparkContext
  @transient implicit val sql:SQLContext = spark.sqlContext
  sc.setLogLevel("WARN")
  //Added new
  //sql.setConf("spark.sql.autoBroadcastJoinThreshold", "-1")
}


class betweenessSpark extends Setter with Serializable {

  val schemaV: StructType = StructType(Seq(StructField("id", StringType), StructField("name", StringType)))
  val schemaE: StructType = StructType(Seq(StructField("src", StringType), StructField("dst", StringType), StructField("relationship", StringType)))

  var V: DataFrame = spark.createDataFrame(sc.emptyRDD[Row], schemaV)
  var E: DataFrame = spark.createDataFrame(sc.emptyRDD[Row], schemaE)

  val schemaAGG_REGISTER:StructType = StructType(Seq(StructField("src",StringType),StructField("dst",StringType),StructField("via",StringType)))
  var AGG_REGISTER:DataFrame = spark.createDataFrame(sc.emptyRDD[Row],schemaAGG_REGISTER)

  val schemaYET:StructType = StructType(Seq(StructField("src",StringType),StructField("dst",StringType)))
  var EXPLORED_REGISTER:DataFrame = spark.createDataFrame(sc.emptyRDD[Row],schemaYET)


  @transient var GlobalGraph: GraphFrame = GraphFrame(V, E)

  /** GraphReader reads the csv file and generate the Graph
   * Inputs:
   * path: Path of the data file
   * */
  def GraphReader(path: String): List[DataFrame] = {

    val df = spark.read.format("csv").option("header", "true").load(path)

    val v1 = df.withColumnRenamed("src", "id").drop("dst", "count");
    val v2 = df.select("dst").withColumnRenamed("dst", "id")
    val v = v1.union(v2)
    List(v, df)
  }


  def calculateBetweeness(G:List[DataFrame],network_type:String):DataFrame={
    var FULL_EDGE_LIST = G(1);
    if(network_type == "directed"){
      FULL_EDGE_LIST = G(1).drop("count")
    }else{
      val tempTable = G(1).select("dst","src").withColumnRenamed("dst","src").withColumnRenamed("src","dst")
      FULL_EDGE_LIST = G(1).drop("count").union(tempTable)
    }
    val FULL_EDGE_LIST_PARTITIONED = FULL_EDGE_LIST.repartition(col("src")).cache()
    val nodes = G(0).distinct().withColumnRenamed("id","src").repartition(col("src"))
    val NOS_NODES = nodes.count();
    val TOTAL_EDGES_TO_EXPLORE = (((NOS_NODES * NOS_NODES) - NOS_NODES) - FULL_EDGE_LIST.count())
    var EXPLORED_NODES_COUNT:Long = 0
    var EDGELIST = FULL_EDGE_LIST_PARTITIONED.withColumnRenamed("src","src_new").withColumnRenamed("dst","dst_new")
    while(((TOTAL_EDGES_TO_EXPLORE - EXPLORED_NODES_COUNT)!=0)){
      EDGELIST = EDGELIST.repartition(col("dst_new")).cache();
      val HOPPED_NEIGHBOR = EDGELIST.join(FULL_EDGE_LIST_PARTITIONED,EDGELIST("dst_new") === FULL_EDGE_LIST_PARTITIONED("src"));
      var AGG = HOPPED_NEIGHBOR.filter(col("src_new")=!=col("dst")).select("src_new","dst","dst_new").withColumnRenamed("src_new","src").withColumnRenamed("dst_new","via")
      val AGG_TEMP = AGG.select("src","dst").except(FULL_EDGE_LIST_PARTITIONED).withColumnRenamed("src","srcs").withColumnRenamed("dst","dsts")
      AGG = AGG.join(AGG_TEMP,AGG("src") === AGG_TEMP("srcs") and AGG("dst") === AGG_TEMP("dsts")).select("src","dst","via")
      AGG_REGISTER = AGG_REGISTER.union(AGG)
      val FILTERED_AGG = AGG.select("src","dst")
      EXPLORED_REGISTER = EXPLORED_REGISTER.union(FILTERED_AGG).distinct()
      EXPLORED_NODES_COUNT = EXPLORED_REGISTER.count()
      EDGELIST.unpersist()
      EDGELIST = FILTERED_AGG.withColumnRenamed("src","src_new").withColumnRenamed("dst","dst_new");
      }
    EXPLORED_REGISTER.unpersist()
    FULL_EDGE_LIST_PARTITIONED.unpersist()
    AGG_REGISTER.cache()
    var AGG_REGISTER_GROUPED_BY_COUNT = AGG_REGISTER.groupBy("src","dst").count().withColumnRenamed("src","src_agg").withColumnRenamed("dst","dst_agg")
    AGG_REGISTER = AGG_REGISTER.repartition(col("src"),col("dst"));
    AGG_REGISTER_GROUPED_BY_COUNT = AGG_REGISTER_GROUPED_BY_COUNT.repartition(col("src_agg"),col("dst_agg"))
    var OUTPUT = AGG_REGISTER.join(AGG_REGISTER_GROUPED_BY_COUNT,AGG_REGISTER("src")===AGG_REGISTER_GROUPED_BY_COUNT("src_agg") and(AGG_REGISTER("dst")===AGG_REGISTER_GROUPED_BY_COUNT("dst_agg"))).select("src","dst","via","count").withColumnRenamed("via","id").distinct()
    OUTPUT = OUTPUT.withColumn("score",expr("1/count")).groupBy("id").sum("score").withColumnRenamed("sum(score)","score")
    val nodes_to_be_zeroed = nodes.except(OUTPUT.select("id")).withColumn("score",lit(0))
    OUTPUT = OUTPUT.union(nodes_to_be_zeroed)
    OUTPUT = OUTPUT.orderBy(desc("score"))
    OUTPUT
  }
}

