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

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Pattern;
import java.util.Properties;

public class BigQueryToIcebergConfig {

  static final ObjectMapper mapper =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  @JsonProperty(value = PROJECT_ID_PROP)
  @NotEmpty
  private String projectId;

  @JsonProperty(value = SPARK_LOG_LEVEL, defaultValue = "INFO")
  @Pattern(regexp = "ALL|DEBUG|ERROR|FATAL|INFO|OFF|TRACE|WARN")
  private String sparkLogLevel;

  @JsonProperty(value = INPUT_BQ_DATASET_ID)
  @NotEmpty
  private String inputBigQueryDatasetId;

  @JsonProperty(value = INPUT_BQ_TABLE_NAME)
  @NotEmpty
  private String inputBigQueryTableName;

  @JsonProperty(value = ICEBERG_OUTPUT_MODE)
  @NotEmpty
  @Pattern(regexp = "(?i)(Overwrite|ErrorIfExists|Append|Ignore)")
  private String icebergWriteMode;

  @JsonProperty(value = ICEBERG_GCS_WAREHOUSE_LOCATION)
  @Pattern(regexp = "gs://(.*?)/(.*)")
  @NotEmpty
  private String icebergGcsWarehouseLocation;

  @JsonProperty(value = ICEBERG_CATALOG_NAME)
  @NotEmpty
  private String icebergCatalogName;

  @JsonProperty(value = ICEBERG_DATABASE_NAME)
  @NotEmpty
  private String icebergDatabase;

  @JsonProperty(value = ICEBERG_TABLE_NAME)
  @NotEmpty
  private String icebergTable;

  public String getProjectId() {
    return projectId;
  }

  public void setProjectId(String projectId) {
    this.projectId = projectId;
  }

  public String getSparkLogLevel() {
    return sparkLogLevel;
  }

  public void setSparkLogLevel(String sparkLogLevel) {
    this.sparkLogLevel = sparkLogLevel;
  }

  public String getInputBigQueryDatasetId() {
    return inputBigQueryDatasetId;
  }

  public void setInputBigQueryDatasetId(String inputBigQueryDatasetId) {
    this.inputBigQueryDatasetId = inputBigQueryDatasetId;
  }

  public String getInputBigQueryTableName() {
    return inputBigQueryTableName;
  }

  public void setInputBigQueryTableName(String inputBigQueryTableName) {
    this.inputBigQueryTableName = inputBigQueryTableName;
  }

  public String getIcebergWriteMode() {
    return icebergWriteMode;
  }

  public void setIcebergWriteMode(String icebergWriteMode) {
    this.icebergWriteMode = icebergWriteMode;
  }

  public String getIcebergGcsWarehouseLocation() {
    return icebergGcsWarehouseLocation;
  }

  public void setIcebergGcsWarehouseLocation(String icebergGcsWarehouseLocation) {
    this.icebergGcsWarehouseLocation = icebergGcsWarehouseLocation;
  }

  public String getIcebergCatalogName() {
    return icebergCatalogName;
  }

  public void setIcebergCatalogName(String icebergCatalogName) {
    this.icebergCatalogName = icebergCatalogName;
  }

  public String getIcebergDatabase() {
    return icebergDatabase;
  }

  public void setIcebergDatabase(String icebergDatabase) {
    this.icebergDatabase = icebergDatabase;
  }

  public String getIcebergTable() {
    return icebergTable;
  }

  public void setIcebergTable(String icebergTable) {
    this.icebergTable = icebergTable;
  }

  @Override
  public String toString() {
    return "BigQueryToIcebergConfig{"
        + "projectId='"
        + projectId
        + '\''
        + ", sparkLogLevel='"
        + sparkLogLevel
        + '\''
        + ", inputBigQueryDatasetId='"
        + inputBigQueryDatasetId
        + '\''
        + ", inputBigQueryTableName='"
        + inputBigQueryTableName
        + '\''
        + ", icebergWriteMode='"
        + icebergWriteMode
        + '\''
        + ", icebergGcsWarehouseLocation='"
        + icebergGcsWarehouseLocation
        + '\''
        + ", icebergCatalogName='"
        + icebergCatalogName
        + '\''
        + ", icebergDatabase='"
        + icebergDatabase
        + '\''
        + ", icebergTable='"
        + icebergTable
        + '\''
        + '}';
  }

  public static BigQueryToIcebergConfig fromProperties(Properties properties) {
    return mapper.convertValue(properties, BigQueryToIcebergConfig.class);
  }
}
