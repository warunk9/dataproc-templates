/*
 * Copyright (C) 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataproc.templates.api;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.API_SECRET_KEY;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.API_INITIAL_COLLECTION;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.API_BASE_URL;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.API_BATCH_SIZE;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.API_TO_GCS_OUTPUT_LOCATION;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.API_TO_GCS_OUTPUT_FORMAT;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.API_TO_GCS_WRITE_MODE;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


import com.google.cloud.dataproc.templates.util.PropertyUtil;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class APIToGCSTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(APIToGCSTest.class);

  @BeforeEach
  void setup() {
    PropertyUtil.getProperties().setProperty(API_SECRET_KEY, "test-secret-key");
    PropertyUtil.getProperties().setProperty(API_INITIAL_COLLECTION, "collection-name");
    PropertyUtil.getProperties().setProperty(API_BASE_URL, "https://api.example.com/v1");
    PropertyUtil.getProperties().setProperty(API_BATCH_SIZE, "100");
    PropertyUtil.getProperties().setProperty(API_TO_GCS_OUTPUT_LOCATION, "gs://test-bucket/output");
    PropertyUtil.getProperties().setProperty(API_TO_GCS_OUTPUT_FORMAT, "avro");
    PropertyUtil.getProperties().setProperty(API_TO_GCS_WRITE_MODE, "append");
  }

  @Test
  void runTemplateWithValidParameters() {
    LOGGER.info("Running test: runTemplateWithValidParameters");
    APIToGCS template = new APIToGCS();
    assertDoesNotThrow(template::validateInput);
  }

  @ParameterizedTest
  @MethodSource("propertyKeys")
  void runTemplateWithInvalidParameters(String propKey) {
    LOGGER.info("Running test: runTemplateWithInvalidParameters");
    PropertyUtil.getProperties().setProperty(propKey, "");
    APIToGCS template = new APIToGCS();
    Exception exception = assertThrows(IllegalArgumentException.class, template::validateInput);
    assertEquals(
        "Required parameters for ApiToGCS not passed. "
            + "Set mandatory parameter for ApiToGCS template in "
            + "resources/conf/template.properties file.",
        exception.getMessage());
  }

  static Stream<String> propertyKeys() {
    return Stream.of(API_SECRET_KEY, API_INITIAL_COLLECTION, API_BASE_URL, API_TO_GCS_OUTPUT_LOCATION, API_TO_GCS_OUTPUT_FORMAT, API_TO_GCS_WRITE_MODE);
  }
}
