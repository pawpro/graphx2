/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.graphx.lib

import org.apache.spark.SparkContext._
import org.apache.spark._
import scala.math._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._


/**
 * Computes the PageRank of URLs from an input file. Input file should
 * be in format of:
 * URL         neighbor URL
 * URL         neighbor URL
 * URL         neighbor URL
 * ...
 * where URL and their neighbors are separated by space(s).
 */
object DataflowPagerank extends Logging {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: PageRank <master> <file> <number_of_iterations>")
      System.exit(1)
    }
    val host = args(0)
    val fname = args(1)
    val iters = args(2).toInt
    val partitions = args(3).toInt
    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "org.apache.spark.graphx.GraphKryoRegistrator")
      .set("spark.locality.wait", "100000")
    // val sc = new SparkContext(args(0), "PageRank",
    //   System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))

    // val sc = new SparkContext(host, "DataflowPagerank(" + fname + ")", conf)
    val sc = new SparkContext(host, "PageRank(" + fname + ")", conf)
    val lines = sc.textFile(fname).repartition(partitions)
    val links: RDD[(Long, Seq[Long])] = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(0).toLong, parts(1).toLong)
    }.groupByKey().cache()
    var ranks: RDD[(Long, Double)] = links.mapValues(v => 1.0)
    logWarning("Graph loaded")

    for (i <- 1 to iters) {
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
      ranks.count
      logWarning(s"Pagerank finished iteration $i")
    }

    // val output = ranks.collect()
    // output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))
    val totalRank = ranks.map{ case(_, r) => r}.reduce(_ + _)
    logWarning(s"Total Pagerank: $totalRank")
    sc.stop()
    


    System.exit(0)
  }
}
