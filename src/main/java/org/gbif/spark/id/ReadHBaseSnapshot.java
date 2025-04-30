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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;

import scala.Tuple2;

// THIS DOES NOT WORK - results in 0 rows (HDFS links not followed?)
public class ReadHBaseSnapshot implements Serializable {

  public static void main(String[] args) throws IOException {
    SparkSession spark = SparkSession.builder().getOrCreate();
    SparkContext sc = spark.sparkContext();
    Configuration hConf = HBaseConfiguration.create(spark.sparkContext().hadoopConfiguration());

    hConf.set("hbase.rootdir", "/hbase");
    hConf.set(
        "hbase.zookeeper.quorum",
        "sc4n1.gbif.org:30578,sc4n3.gbif.org:30578,uc6n10.gbif.org:30578,uc6n11.gbif.org:30578,uc6n12.gbif.org:30578");
    hConf.set(TableInputFormat.SCAN, convertScanToString(new Scan().setMaxVersions(3)));

    Job job = Job.getInstance(hConf);

    Path path = new Path("hdfs:///tmp/tim");
    String snapName = "tim_lookup";
    TableSnapshotInputFormat.setInput(job, snapName, path);

    RDD<Tuple2<ImmutableBytesWritable, Result>> hBaseRDD =
        sc.newAPIHadoopRDD(
            job.getConfiguration(),
            TableSnapshotInputFormat.class,
            ImmutableBytesWritable.class,
            Result.class);

    long record_count_raw = hBaseRDD.count();
    System.out.println(
        "[ *** ] Read in SnapShot ("
            + snapName
            + "), which contains "
            + record_count_raw
            + " records");
  }

  static String convertScanToString(Scan scan) throws IOException {
    return Base64.getEncoder().encodeToString(ProtobufUtil.toScan(scan).toByteArray());
  }
}
