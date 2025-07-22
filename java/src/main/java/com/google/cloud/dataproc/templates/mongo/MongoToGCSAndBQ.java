/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.dataproc.templates.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.google.cloud.dataproc.templates.BaseTemplate;
import com.google.cloud.dataproc.templates.util.PropertyUtil;
import com.google.cloud.dataproc.templates.util.TemplateConstants;
import com.google.cloud.dataproc.templates.util.ValidationUtil;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.types.DataTypes;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoToGCSAndBQ implements BaseTemplate {

  public static final Logger LOGGER = LoggerFactory.getLogger(MongoToGCSAndBQ.class);
  private final MongoToGCSAndBQConfig config;

  // private final String bqTableName;
  // private final String bqOutputMode;
  private final String gcsOutputFormat;
  private final String gcsDelimiter;
  private final String gcsOutputMode;
  private final String bqOutputMode;
  private final String gcsOutputLocation;

  public MongoToGCSAndBQ(MongoToGCSAndBQConfig config) {

    this.config = config;
    this.gcsOutputFormat =
        config.getGcsOutputFormat() == null
            ? TemplateConstants.SPARK_FILE_FORMAT_CSV
            : config.getGcsOutputFormat();

    this.gcsDelimiter =
        config.getGcsDelimiter() == null
            ? TemplateConstants.GCS_DEFAULT_DELIMITTER
            : config.getGcsDelimiter();
    this.gcsOutputMode = config.getGcsWriteMode();
    this.bqOutputMode = config.getBigQueryOutputMode();
    this.gcsOutputLocation = config.getGcsOutputLocation();
  }

  public static MongoToGCSAndBQ of(String... args) {
    MongoToGCSAndBQConfig mongoToBQConfig =
        MongoToGCSAndBQConfig.fromProperties(PropertyUtil.getProperties());
    LOGGER.info("Config loaded\n{}", mongoToBQConfig);
    return new MongoToGCSAndBQ(mongoToBQConfig);
  }

  @Override
  public void validateInput() throws IllegalArgumentException {
    ValidationUtil.validateOrThrow(config);
  }

  @Override
  public void runTemplate()
      throws StreamingQueryException, TimeoutException, SQLException, InterruptedException {

    LOGGER.info("Initialize the Spark session");
    SparkSession spark =
        SparkSession.builder()
            .appName("Spark MongoToBQ Job")
            .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
            .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
            .getOrCreate();

    long targetFileSizeGb = 2;
    long targetFileSizeBytes = targetFileSizeGb * 1024 * 1024 * 1024;
    StructType schema = null;
    MongoClient mongoClient = MongoClients.create(config.getInputURI());
    MongoDatabase database = mongoClient.getDatabase(config.getInputDatabase());
    MongoCollection<Document> collection = database.getCollection(config.getInputCollection());

    // 2. Fetch a sample document (e.g., the first document)
    Document sampleDocument = collection.find().first();
    Document collStatsCommand = new Document("collStats", config.getInputCollection());
    Document statsResult = database.runCommand(collStatsCommand);
    // long storageSizeInBytes = statsResult.getLong("storageSize");
    long sizeInBytes = statsResult.getLong("size");

    if (sampleDocument != null) {
      // 3. Create the StructType schema iteratively from the Document

      schema = createStructTypeFromDocument(sampleDocument);

      // 4. Print the schema
      LOGGER.info("Schema tree structure: ");
      schema.printTreeString();
    }

    LOGGER.info("Read from Mongo");

    Dataset<Row> rowDataset =
        spark
            .read()
            .format(TemplateConstants.MONGO_FORMAT)
            .schema(schema)
            .option(TemplateConstants.MONGO_INPUT_URI, config.getInputURI())
            .option(TemplateConstants.MONGO_DATABASE, config.getInputDatabase())
            .option(TemplateConstants.MONGO_COLLECTION, config.getInputCollection())
            .load();

    LOGGER.info("Write to BigQuery");

    int numP = (int) Math.floor((double) sizeInBytes / targetFileSizeBytes);
    int numPartitions = (Math.max(1, numP));
    LOGGER.info("Repartitioning data for GCS output into {} partitions.", numPartitions);
    Dataset<Row> finalSqlDf = rowDataset.repartition(numPartitions);

    Runnable writeToGcsTask =
        () -> {
          try {
            finalSqlDf
                .write()
                .format(gcsOutputFormat)
                .option("delimiter", gcsDelimiter)
                .option("header", "true")
                .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
                .mode(gcsOutputMode.toLowerCase())
                .save(gcsOutputLocation);
            LOGGER.info("Successfully wrote data to GCS: ");
          } catch (Exception e) {
            LOGGER.error("Error writing to GCS: " + e.getMessage());
            throw new RuntimeException("Error writing to GCS. Terminating job.", e);
          }
        };

    Runnable writeToBigQueryTask =
        () -> {
          try {
            rowDataset
                .write()
                .format("bigquery")
                .option(
                    "table",
                    String.format(
                        TemplateConstants.BQ_TABLE_NAME_FORMAT,
                        config.getProjectId(),
                        config.getBqOutputDataset(),
                        config.getBqOutputTable()))
                .option("temporaryGcsBucket", config.getBqTempBucket())
                .option("writeMethod", "indirect")
                .option("intermediateFormat", "avro")
                .mode(bqOutputMode.toLowerCase())
                .save();
            LOGGER.info("Successfully wrote data to BigQuery: ");

          } catch (Exception e) {
            LOGGER.error("Error writing to BigQuery: " + e.getMessage());
            throw new RuntimeException("Error writing to BigQuery. Terminating job.", e);
          }
        };

    // Execute the write operations in parallel using CompletableFuture
    CompletableFuture<Void> gcsFuture = CompletableFuture.runAsync(writeToGcsTask);
    CompletableFuture<Void> bigQueryFuture = CompletableFuture.runAsync(writeToBigQueryTask);

    // Wait for both operations to complete
    CompletableFuture.allOf(gcsFuture, bigQueryFuture).join();

    LOGGER.info("Data successfully loaded to both GCS and BigQuery in parallel.");

    // Stop the SparkSession
    spark.stop();
  }

  private static StructType createStructTypeFromDocument(Document document) {
    List<StructField> fields = new ArrayList<>();
    Metadata jsonMetadata =
        new MetadataBuilder()
            .putString("sqlType", "JSON") // Explicitly set BigQuery type to JSON
            .build();
    for (Map.Entry<String, Object> entry : document.entrySet()) {
      String fieldName = entry.getKey();
      Object fieldValue = entry.getValue();
      fields.add(createStructField(fieldName, fieldValue, jsonMetadata));
    }
    return DataTypes.createStructType(fields);
  }

  private static StructField createStructField(
      String fieldName, Object fieldValue, Metadata jsonMetadata) {
    if (fieldValue == null) {
      return DataTypes.createStructField(fieldName, DataTypes.StringType, true);
    } else if (fieldValue instanceof String) {
      return DataTypes.createStructField(fieldName, DataTypes.StringType, true);
    } else if (fieldValue instanceof Integer) {
      return DataTypes.createStructField(fieldName, DataTypes.IntegerType, true);
    } else if (fieldValue instanceof Long) {
      return DataTypes.createStructField(fieldName, DataTypes.LongType, true);
    } else if (fieldValue instanceof Double) {
      return DataTypes.createStructField(fieldName, DataTypes.DoubleType, true);
    } else if (fieldValue instanceof Boolean) {
      return DataTypes.createStructField(fieldName, DataTypes.BooleanType, true);
    } else if (fieldValue instanceof java.util.Date) {
      return DataTypes.createStructField(fieldName, DataTypes.TimestampType, true);
    } else if (fieldValue instanceof ObjectId) {
      return DataTypes.createStructField(fieldName, DataTypes.StringType, true);
    } else if (fieldValue instanceof Document) {
      return DataTypes.createStructField(fieldName, DataTypes.StringType, true, jsonMetadata);
    } else {
      return DataTypes.createStructField(fieldName, DataTypes.StringType, true, jsonMetadata);
    }
  }
}
