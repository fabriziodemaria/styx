/*-
 * -\-\-
 * Spotify Styx Common
 * --
 * Copyright (C) 2016 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.styx.state;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;

/**
 * A value type for holding a message.
 */
@AutoValue
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class Message {

  @JsonProperty
  public abstract MessageLevel level();

  @JsonProperty
  public abstract String line();

  @JsonCreator
  public static Message create(
      @JsonProperty("level") MessageLevel level,
      @JsonProperty("line") String line) {
    return new AutoValue_Message(level, line);
  }

  public static Message info(String line) {
    return create(MessageLevel.INFO, line);
  }

  public static Message warning(String line) {
    return create(MessageLevel.WARNING, line);
  }

  public static Message error(String line) {
    return create(MessageLevel.ERROR, line);
  }

  public enum MessageLevel {
    INFO, WARNING, ERROR, UNKNOWN;

    @JsonCreator
    public static MessageLevel forValue(String value) {
      try {
        return valueOf(value);
      } catch (IllegalArgumentException ignore) {
        return UNKNOWN;
      }
    }
  }
}
