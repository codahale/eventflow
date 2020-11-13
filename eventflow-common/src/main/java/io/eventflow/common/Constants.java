/*
 * Copyright © 2020 Coda Hale (coda.hale@gmail.com)
 *
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
package io.eventflow.common;

public class Constants {
  public static final String ID_ATTRIBUTE = "event.id";
  public static final String TIMESTAMP_ATTRIBUTE = "event.timestamp";
  public static final String TYPE_ATTRIBUTE = "event.type";
  public static final String SOURCE_ATTRIBUTE = "event.source";
  public static final String CUSTOMER_ATTRIBUTE = "event.customer";

  private Constants() {
    // singleton
  }
}
