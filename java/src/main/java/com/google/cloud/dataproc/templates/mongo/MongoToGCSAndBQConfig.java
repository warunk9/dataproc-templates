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

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Pattern;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;

public class MongoToGCSAndBQConfig {

  static final ObjectMapper mapper =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  @JsonProperty(value = PROJECT_ID_PROP)
  @NotEmpty
  private String projectId;

  @JsonProperty(value = MONGO_INPUT_URI)
  @NotEmpty
  private String inputURI;

  @JsonProperty(value = MONGO_INPUT_DATABASE)
  @NotEmpty
  private String inputDatabase;

  @JsonProperty(value = MONGO_INPUT_COLLECTION)
  @NotEmpty
  private String inputCollection;

  @JsonProperty(value = GCS_WRITE_MODE)
  @NotEmpty
  @Pattern(regexp = "(?i)(Overwrite|ErrorIfExists|Append|Ignore)")
  private String gcsWriteMode;

  @JsonProperty(value = BQ_WRITE_MODE)
  @NotEmpty
  @Pattern(regexp = "(?i)(Overwrite|ErrorIfExists|Append)")
  private String bigQueryOutputMode;

  @JsonProperty(value = MONGO_BQ_OUTPUT_DATASET)
  @NotEmpty
  private String bqOutputDataset;

  @JsonProperty(value = MONGO_BQ_OUTPUT_TABLE)
  @NotEmpty
  private String bqOutputTable;

  @JsonProperty(value = MONGO_BQ_TEMP_BUCKET_NAME)
  @NotEmpty
  private String bqTempBucket;

  @JsonProperty(value = GCS_OUTPUT_FORMAT)
  @NotEmpty
  @Pattern(regexp = "csv|avro|orc|json|parquet")
  private String gcsOutputFormat;

  // Optional GCS delimiter
  @JsonProperty(value = GCS_DELIMITER)
  private String gcsDelimiter;

  @JsonProperty(value = GCS_OUTPUT_LOCATION)
  @Pattern(regexp = "gs://(.*?)/(.*)")
  @NotEmpty
  private String gcsOutputLocation;

  public @NotEmpty String getProjectId() {
    return projectId;
  }

  public @NotEmpty String getInputURI() {
    return inputURI;
  }

  public @NotEmpty String getInputDatabase() {
    return inputDatabase;
  }

  public @NotEmpty String getInputCollection() {
    return inputCollection;
  }

  public @NotEmpty String getBqOutputDataset() {
    return bqOutputDataset;
  }

  public @NotEmpty String getBqOutputTable() {
    return bqOutputTable;
  }

  public @NotEmpty String getBqTempBucket() {
    return bqTempBucket;
  }

  public String getGcsOutputFormat() {
    return gcsOutputFormat;
  }

  public String getGcsWriteMode() {
    return gcsWriteMode;
  }

  public String getGcsDelimiter() {
    return gcsDelimiter;
  }

  public String getBigQueryOutputMode() {
    return bigQueryOutputMode;
  }

  public String getGcsOutputLocation() {
    return gcsOutputLocation;
  }

  @Override
  public String toString() {
    return "MongoToBQConfig{"
        + "projectId='"
        + projectId
        + '\''
        + ", inputURI='"
        + inputURI
        + '\''
        + ", inputDatabase='"
        + inputDatabase
        + '\''
        + ", inputCollection='"
        + inputCollection
        + '\''
        + ", bqOutputMode='"
        + bigQueryOutputMode
        + '\''
        + ", gcsOutputMode='"
        + gcsWriteMode
        + '\''
        + ", bqOutputDataset='"
        + bqOutputDataset
        + '\''
        + ", bqOutputTable='"
        + bqOutputTable
        + '\''
        + ", bqTempBucket='"
        + bqTempBucket
        + '\''
        + '}';
  }

  public static MongoToGCSAndBQConfig fromProperties(Properties properties) {
    return mapper.convertValue(properties, MongoToGCSAndBQConfig.class);
  }

  @AssertTrue(
      message =
          "Required parameters for MongoToBQ not passed. Refer to databases/README.md for more instructions.")
  private boolean isInputValid() {
    return StringUtils.isNotBlank(inputURI)
        && StringUtils.isNotBlank(inputDatabase)
        && StringUtils.isNotBlank(inputCollection)
        && StringUtils.isNotBlank(bigQueryOutputMode)
        && StringUtils.isNotBlank(gcsWriteMode)
        && StringUtils.isNotBlank(bqOutputDataset)
        && StringUtils.isNotBlank(bqOutputTable)
        && StringUtils.isNotBlank(bqTempBucket);
  }
}
