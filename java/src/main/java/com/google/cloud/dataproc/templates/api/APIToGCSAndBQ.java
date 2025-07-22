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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.dataproc.templates.BaseTemplate;
import com.google.cloud.dataproc.templates.util.PropertyUtil;
import com.google.cloud.dataproc.templates.util.TemplateConstants;
import com.google.cloud.dataproc.templates.util.ValidationUtil;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import okhttp3.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class APIToGCSAndBQ implements BaseTemplate {

  private static final Logger LOGGER = LoggerFactory.getLogger(APIToGCSAndBQ.class);

  private final APIToGCSAndBQConfig config;
  private final String gcsOutputLocation;
  private final String bqTableName;
  private final String tempGcsBucket;
  private final String gcsOutputMode;
  private final String gcsDelimiter;
  private final String bqOutputMode;
  private final String baseUrl;
  private final String tokenUrl;
  private final String apiInitialCollection;
  private final String secretKey;
  private int batchSize;

  private final SparkSession spark;

  public APIToGCSAndBQ(APIToGCSAndBQConfig config) {

    this.config = config;

    this.baseUrl = config.getBaseUrl();
    this.tokenUrl = baseUrl + "tokens";
    this.apiInitialCollection = config.getApiInitialCollection();
    this.secretKey = config.getApiSecretKey();

    this.gcsOutputLocation = config.getGcsOutputLocation();
    this.gcsDelimiter =
        config.getGcsDelimiter() == null
            ? TemplateConstants.GCS_DEFAULT_DELIMITTER
            : config.getGcsDelimiter();

    this.bqTableName =
        String.format(
            TemplateConstants.BQ_TABLE_NAME_FORMAT,
            config.getProjectId(),
            config.getBigQueryDatasetId(),
            config.getBigQueryTableName());
    this.tempGcsBucket = config.getTempGcsBucket();
    this.gcsOutputMode = config.getGcsWriteMode();
    this.bqOutputMode = config.getBigQueryOutputMode();
    this.batchSize = config.getBatchSize();

    this.spark =
        SparkSession.builder()
            .appName("Spark APIToGCSAndBQ")
            .config("temporaryGcsBucket", tempGcsBucket)
            .getOrCreate();

    this.spark.sparkContext().setLogLevel("INFO");
  }

  public static APIToGCSAndBQ of(String... args) {
    APIToGCSAndBQConfig config = APIToGCSAndBQConfig.fromProperties(PropertyUtil.getProperties());
    ValidationUtil.validateOrThrow(config);
    LOGGER.info("Config loaded\n{}", config);
    return new APIToGCSAndBQ(config);
  }

  @Override
  public void runTemplate() {
    try {
      String accessToken = getAccessToken();
      List<Dataset<Row>> allPageDataFrames = new ArrayList<Dataset<Row>>();
      String nextUrl = baseUrl + apiInitialCollection;

      int ctr = 1;
      StructType schema = null;

      do {

        ApiResponse apiResponse = fetchAPIData(accessToken, nextUrl);
        String jsonData = apiResponse.getJsonData();
        String nextPageLink = apiResponse.getNextPageLink();

        if (jsonData != null && !jsonData.isEmpty()) {
          Dataset<Row> pageDF = parseJsonToDataFrame(jsonData, schema);
          if (ctr == 1) {
            schema = pageDF.schema();
            ctr = 0;
          }
          if (pageDF.count() > 0) {
            allPageDataFrames.add(pageDF);
          }

          if (allPageDataFrames.size() == batchSize || pageDF.count() == 0) {

            Dataset<Row> finalDF = allPageDataFrames.get(0);
            for (int i = 1; i < allPageDataFrames.size(); i++) {
              finalDF =
                  finalDF.unionByName(
                      allPageDataFrames.get(i),
                      true); // Use true for allowMissingColumns if schemas might differ
            }

            Dataset<Row> resultDf = finalDF.repartition(1);

            try {
              CompletableFuture<Void> gcsFuture =
                  CompletableFuture.runAsync(() -> writeToGCS(resultDf));
              CompletableFuture<Void> bqFuture =
                  CompletableFuture.runAsync(() -> writeToBigQuery(resultDf));

              CompletableFuture.allOf(gcsFuture, bqFuture).join();

              LOGGER.info("Successfully wrote API data to GCS and BigQuery.");
            } catch (Exception e) {
              LOGGER.error("Error writing to GCS and BigQuery", e);
              spark.stop();
              throw new RuntimeException(e);
            }

            allPageDataFrames.clear();
          }

          if (pageDF.count() == 0) {
            break;
          }

        } else {
          throw new RuntimeException("API returned no data.");
        }

        if (nextPageLink != null && !nextPageLink.isEmpty()) {
          nextUrl = baseUrl + nextPageLink;
        } else {
          nextUrl = null; // No more pages
        }

      } while (nextUrl != null);

    } catch (Exception e) {
      LOGGER.error("Failed to process API data", e);
      System.exit(1);
    } finally {
      spark.stop();
    }
  }

  public void validateInput() {

    ValidationUtil.validateOrThrow(config);
  }

  private String getAccessToken() throws IOException {
    OkHttpClient client = new OkHttpClient();
    MediaType JSON = MediaType.parse("application/json");

    RequestBody body = RequestBody.create("{\"secretKey\": \"" + secretKey + "\"}", JSON);

    Request request =
        new Request.Builder()
            .url(tokenUrl)
            .post(body)
            .addHeader("Content-Type", "application/json")
            .build();

    try (Response response = client.newCall(request).execute()) {

      if (!response.isSuccessful()) throw new IOException("Unexpected code " + response);

      ObjectMapper mapper = new ObjectMapper();
      JsonNode jsonNode = mapper.readTree(response.body().string());

      return jsonNode.get("token").asText();
    }
  }

  private Dataset<Row> parseJsonToDataFrame(String jsonData, StructType schema) {
    Dataset<String> jsonDS =
        spark.createDataset(java.util.Collections.singletonList(jsonData), Encoders.STRING());

    if (schema == null) {
      return spark.read().json(jsonDS);
    } else {
      return spark.read().schema(schema).json(jsonDS);
    }
  }

  private void writeToGCS(Dataset<Row> df) {
    try {
      df.write()
          .format(config.getGcsOutputFormat())
          .option("delimiter", gcsDelimiter)
          .option("header", "true")
          .mode(gcsOutputMode)
          .save(gcsOutputLocation);
      LOGGER.info("Data successfully written to GCS");
    } catch (Exception e) {
      LOGGER.error("Error writing to GCS", e);
      throw new RuntimeException(e);
    }
  }

  private void writeToBigQuery(Dataset<Row> df) {
    try {
      df.write()
          .format("bigquery")
          .option("table", bqTableName)
          .option("temporaryGcsBucket", tempGcsBucket)
          .mode(bqOutputMode)
          .save();
      LOGGER.info("Data successfully written to BigQuery");
    } catch (Exception e) {
      LOGGER.error("Error writing to BigQuery", e);
      throw new RuntimeException(e);
    }
  }

  private ApiResponse fetchAPIData(String token, String url) throws IOException {
    OkHttpClient client = new OkHttpClient();

    Request request =
        new Request.Builder()
            .url(url)
            .get()
            .addHeader("Authorization", "Bearer " + token)
            .addHeader("Content-Type", "application/json")
            .build();

    try (Response response = client.newCall(request).execute()) {
      if (!response.isSuccessful())
        throw new IOException("Failed to fetch data from API: " + response);

      String jsonData = response.body().string();
      String nextPageLink = extractNextPageLink(response.headers().toMultimap());

      return new ApiResponse(jsonData, nextPageLink);
    }
  }

  private String extractNextPageLink(Map<String, List<String>> headers) {
    if (headers.containsKey("link")) {
      for (String linkHeader : headers.get("link")) {
        if (linkHeader.contains("rel=\"next\"")) {
          int startIndex = linkHeader.indexOf("<") + 1;
          int endIndex = linkHeader.indexOf(">");
          if (startIndex > 0 && endIndex > startIndex) {
            String relativeUrl = linkHeader.substring(startIndex, endIndex);
            // Remove leading slash if present to avoid double slash when appending to BASE_API_URL
            if (relativeUrl.startsWith("/")) {
              relativeUrl = relativeUrl.substring(1);
            }
            return relativeUrl;
          }
        }
      }
    }
    return null;
  }
}
