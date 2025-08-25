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

public class ApiResponse {
  private final String jsonData;
  private final String nextPageLink;

  public ApiResponse(String jsonData, String nextPageLink) {
    this.jsonData = jsonData;
    this.nextPageLink = nextPageLink;
  }

  public String getJsonData() {
    return jsonData;
  }

  public String getNextPageLink() {
    return nextPageLink;
  }
}
