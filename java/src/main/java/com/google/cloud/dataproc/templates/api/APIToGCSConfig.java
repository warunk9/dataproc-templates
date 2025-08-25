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
package com.google.cloud.dataproc.templates.api;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import java.util.Properties;

public class APIToGCSConfig {
  static final ObjectMapper mapper =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  @JsonProperty(value = PROJECT_ID_PROP)
  @NotEmpty
  private String projectId;

  @JsonProperty(value = API_BASE_URL)
  @NotEmpty
  private String baseUrl;

  @JsonProperty(value = API_SECRET_KEY)
  @NotEmpty
  private String apiSecretKey; // For the Enverus API secret key

  @JsonProperty(value = API_INITIAL_COLLECTION)
  @NotEmpty
  private String apiInitialCollection;

  @JsonProperty(value = API_BATCH_SIZE)
  @NotNull
  @Min(value = 100, message = "Batch size must be at least 100")
  private int batchSize;

  @JsonProperty(value = API_TO_GCS_OUTPUT_LOCATION)
  @Pattern(regexp = "gs://(.*?)/(.*)")
  @NotEmpty
  private String gcsOutputLocation;

  @JsonProperty(value = API_TO_GCS_OUTPUT_FORMAT)
  @NotEmpty
  @Pattern(regexp = "csv|avro|orc|json|parquet")
  private String gcsOutputFormat;

  @JsonProperty(value = API_TO_GCS_WRITE_MODE)
  @NotEmpty
  @Pattern(regexp = "(?i)(Overwrite|ErrorIfExists|Append|Ignore)")
  private String gcsWriteMode;

  @JsonProperty(value = SPARK_LOG_LEVEL, defaultValue = "INFO")
  @Pattern(regexp = "ALL|DEBUG|ERROR|FATAL|INFO|OFF|TRACE|WARN")
  private String sparkLogLevel;

  public String getProjectId() {
    return projectId;
  }

  public void setProjectId(String projectId) {
    this.projectId = projectId;
  }

  public String getBaseUrl() {
    return baseUrl;
  }

  public void setBaseUrl(String baseUrl) {
    this.baseUrl = baseUrl;
  }

  public String getGcsOutputLocation() {
    return gcsOutputLocation;
  }

  public void setGcsOutputLocation(String gcsOutputLocation) {
    this.gcsOutputLocation = gcsOutputLocation;
  }

  public String getGcsOutputFormat() {
    return gcsOutputFormat;
  }

  public void setGcsOutputFormat(String gcsOutputFormat) {
    this.gcsOutputFormat = gcsOutputFormat;
  }

  public String getGcsWriteMode() {
    return gcsWriteMode;
  }

  public void setGcsWriteMode(String gcsWriteMode) {
    this.gcsWriteMode = gcsWriteMode;
  }

  public String getSparkLogLevel() {
    return sparkLogLevel;
  }

  public void setSparkLogLevel(String sparkLogLevel) {
    this.sparkLogLevel = sparkLogLevel;
  }

  public String getApiSecretKey() {
    return apiSecretKey;
  }

  public void setApiSecretKey(String apiSecretKey) {
    this.apiSecretKey = apiSecretKey;
  }

  public String getApiInitialCollection() {
    return apiInitialCollection;
  }

  public void setApiInitialCollection(String apiInitialCollection) {
    this.apiInitialCollection = apiInitialCollection;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public static APIToGCSConfig fromProperties(Properties properties) {
    // Ensure the properties object is compatible with ObjectMapper
    return mapper.convertValue(properties, APIToGCSConfig.class);
  }

  @Override
  public String toString() {
    return "APIToGCSConfig{"
        + "projectId='"
        + projectId
        + '\''
        + ", apiSecretKey='"
        + "********"
        + '\''
        + // Mask sensitive key
        ", apiInitialCollection='"
        + apiInitialCollection
        + '\''
        + ", batchSize='"
        + batchSize
        + '\''
        + ", gcsOutputLocation='"
        + gcsOutputLocation
        + '\''
        + ", gcsOutputFormat='"
        + gcsOutputFormat
        + '\''
        + ", gcsWriteMode='"
        + gcsWriteMode
        + '\''
        + ", sparkLogLevel='"
        + sparkLogLevel
        + '\''
        + '}';
  }
}
