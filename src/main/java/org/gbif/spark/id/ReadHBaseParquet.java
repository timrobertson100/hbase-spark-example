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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Examples for querying the parquet output.
 */
public class ReadHBaseParquet implements Serializable {

  public static void main(String[] args) {
    SparkSession spark = SparkSession.builder().getOrCreate();

    Dataset<Row> data = spark.read().parquet("hdfs:///tmp/lookup.parquet");
    data.createOrReplaceTempView("lookup");

    // dump the partition counts
    spark
        .sql("select substr(key, 0, 3) as salt, count(*) as cnt from lookup group by salt")
        .show(1000);

    // dump the partition counts for iNaturalist
    spark
        .sql(
            "select substr(key, 0, 3) as salt, count(*) as cnt from lookup where key like '%50c9509d-22c7-4a22-a47d-8c48425ef4a7%' group by salt")
        .show(1000);

    // dump the status counts
    spark.sql("select status, count(*) as cnt from lookup group by status").show(1000);
  }
}
