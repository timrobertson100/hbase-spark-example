/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.spark.id;

import java.io.IOException;
import java.io.Serializable;
import java.util.Base64;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

/**
 * Read the identifier table, and write is as parquet
 */
public class ReadHBaseTable implements Serializable {

  public static void main(String[] args) throws IOException {
    SparkSession spark = SparkSession.builder().getOrCreate();
    SparkContext sc = spark.sparkContext();
    Configuration hConf = HBaseConfiguration.create(spark.sparkContext().hadoopConfiguration());

    hConf.set("hbase.rootdir", "/hbase");
    hConf.set(
        "hbase.zookeeper.quorum",
        "c8n1.gbif.org:31930,c8n2.gbif.org:31930,c8n3.gbif.org:31930,c8n5.gbif.org:31930,c8n9.gbif.org:31930");

    hConf.set(TableInputFormat.SCAN, convertScanToString(new Scan()));
    hConf.set(TableInputFormat.INPUT_TABLE, "prod_h_occurrence_lookup");

    Job job = Job.getInstance(hConf);

    JavaSparkContext jsc = new JavaSparkContext(sc);
    JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD =
        jsc.newAPIHadoopRDD(
            job.getConfiguration(),
            TableInputFormat.class,
            ImmutableBytesWritable.class,
            Result.class);

    StructType schema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("key", DataTypes.StringType, false),
              DataTypes.createStructField("status", DataTypes.StringType, true),
              DataTypes.createStructField("gbifID", DataTypes.LongType, true)
            });

    JavaRDD<Row> lookup =
        hBaseRDD.map(
            (Function<Tuple2<ImmutableBytesWritable, Result>, Row>)
                row -> {
                  ImmutableBytesWritable key = row._1();
                  Result result = row._2();

                  String rowKey = Bytes.toString(key.get());
                  byte[] statusBytes = result.getValue(Bytes.toBytes("o"), Bytes.toBytes("s"));
                  String status = statusBytes == null ? null : Bytes.toString(statusBytes);
                  byte[] idBytes = result.getValue(Bytes.toBytes("o"), Bytes.toBytes("i"));
                  Long id = idBytes == null ? null : Bytes.toLong(idBytes);

                  return RowFactory.create(rowKey, status, id);
                });

    Dataset<Row> lookupWithSchema = spark.createDataFrame(lookup, schema);
    lookupWithSchema.write().mode("overwrite").parquet("hdfs:///tmp/lookup.parquet");
  }

  static String convertScanToString(Scan scan) throws IOException {
    return Base64.getEncoder().encodeToString(ProtobufUtil.toScan(scan).toByteArray());
  }
}
