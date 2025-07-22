/*
 * Copyright (C) 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataproc.templates.iceberg;

import com.google.cloud.dataproc.templates.BaseTemplate;
import com.google.cloud.dataproc.templates.util.PropertyUtil;
import com.google.cloud.dataproc.templates.util.TemplateConstants;
import com.google.cloud.dataproc.templates.util.ValidationUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryToIceberg implements BaseTemplate {

  private static final Logger LOGGER = LoggerFactory.getLogger(BigQueryToIceberg.class);

  private final BigQueryToIcebergConfig config;

  private final String bqInputTableName;
  private final String icebergWriteMode;
  private final String icebergGcsWarehouseLocation;
  private final String icebergCatalogName;
  private final String icebergDatabase;
  private final String icebergTable;

  private final String sparkLogLevel;

  public BigQueryToIceberg(BigQueryToIcebergConfig config) {
    this.config = config;
    this.bqInputTableName =
        String.format(
            TemplateConstants.BQ_TABLE_NAME_FORMAT,
            config.getProjectId(),
            config.getInputBigQueryDatasetId(),
            config.getInputBigQueryTableName());

    this.icebergWriteMode = config.getIcebergWriteMode();
    this.icebergGcsWarehouseLocation = config.getIcebergGcsWarehouseLocation();
    this.icebergCatalogName = config.getIcebergCatalogName();
    this.icebergDatabase = config.getIcebergDatabase();
    this.icebergTable = config.getIcebergTable();
    this.sparkLogLevel = config.getSparkLogLevel();
  }

  public static BigQueryToIceberg of(String... args) {
    BigQueryToIcebergConfig config =
        BigQueryToIcebergConfig.fromProperties(PropertyUtil.getProperties());
    ValidationUtil.validateOrThrow(config);
    LOGGER.info("Config loaded\n{}", config);
    return new BigQueryToIceberg(config);
  }

  @Override
  public void runTemplate() {

    long targetFileSizeGb = 2;
    long targetFileSizeBytes = targetFileSizeGb * 1024 * 1024 * 1024;

    String sparkSqlCatalogKey = "spark.sql.catalog." + icebergCatalogName;
    String sparkSqlCatalogKeyType = "spark.sql.catalog." + icebergCatalogName + ".type";
    String sparkSqlCatalogWarehouse = "spark.sql.catalog." + icebergCatalogName + ".warehouse";

    SparkSession spark =
        SparkSession.builder()
            .appName("BigQuery to Iceberg")
            .config("spark.sql.defaultCatalog", icebergCatalogName)
            .config("spark.sql.catalogImplementation", TemplateConstants.IN_MEMORY)
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config(sparkSqlCatalogKey, "org.apache.iceberg.spark.SparkCatalog")
            .config(sparkSqlCatalogKeyType, TemplateConstants.HADOOP)
            .config(sparkSqlCatalogWarehouse, icebergGcsWarehouseLocation)
            .config("spark.sql.catalog.local.hadoop.fs.defaultFS", TemplateConstants.GS_FS)
            .config("spark.sql.catalog.local.hadoop.default.warehouse", icebergGcsWarehouseLocation)
            .config(
                "spark.sql.catalog.local.hadoop.default.storage.gcs.bucket",
                icebergGcsWarehouseLocation)
            .getOrCreate();

    // Set log level
    spark.sparkContext().setLogLevel(sparkLogLevel);

    String createIcebergDbSql = "CREATE DATABASE IF NOT EXISTS " + icebergDatabase;
    spark.sql(createIcebergDbSql);
    String useIcebergDbSql = "USE " + icebergDatabase;
    spark.sql(useIcebergDbSql);

    Dataset<Row> inputData =
        spark.read().format(TemplateConstants.BIGQUERY_INPUT_FORMAT).load(bqInputTableName);

    String fullyQualifiedIcebergTable = icebergDatabase + "." + icebergTable;

    int numPartitions = calculateNumPartitionsForGCS(inputData, targetFileSizeBytes);
    LOGGER.info("Repartitioning data for GCS output into {} partitions.", numPartitions);
    Dataset<Row> finalIcebergDf = inputData.repartition(numPartitions);

    finalIcebergDf
        .write()
        .format(TemplateConstants.ICEBERG_FORMAT)
        .mode(SaveMode.valueOf(icebergWriteMode))
        .saveAsTable(fullyQualifiedIcebergTable);

    // print few rows for validation
    Dataset<Row> iceberg_df =
        spark.read().format(TemplateConstants.ICEBERG_FORMAT).table(fullyQualifiedIcebergTable);
    iceberg_df.printSchema();
    LOGGER.info("count in iceberg : " + iceberg_df.count());
    iceberg_df.show();
  }

  @Override
  public void validateInput() throws IllegalArgumentException {
    ValidationUtil.validateOrThrow(config);
  }

  private int calculateNumPartitionsForGCS(Dataset<Row> df, long targetSizeBytes) {

    long totalBytes = df.count() * df.schema().defaultSize();
    int numP = (int) Math.floor((double) totalBytes / targetSizeBytes);
    int finalPartition = (Math.max(1, numP));
    LOGGER.info("totalBytes: " + totalBytes + " number of partition :" + finalPartition);

    return finalPartition;
  }
}
