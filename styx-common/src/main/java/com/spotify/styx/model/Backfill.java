/*-
 * -\-\-
 * Spotify Styx Common
 * --
 * Copyright (C) 2017 Spotify AB
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

package com.spotify.styx.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.spotify.styx.util.RandomGenerator;
import java.time.Instant;
import java.util.Optional;

@AutoValue
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class Backfill {

  @JsonProperty
  public abstract String id();

  @JsonProperty
  public abstract Instant start();

  @JsonProperty
  public abstract Instant end();

  @JsonProperty
  public abstract String component();

  @JsonProperty
  public abstract String workflow();

  @JsonProperty
  public abstract long concurrency();

  @JsonProperty
  public abstract Optional<String> dockerImage();

  @JsonProperty
  public abstract boolean processed();

  @JsonProperty
  public abstract String resource();

  @JsonCreator
  public static Backfill create(
      @JsonProperty("id") String id,
      @JsonProperty("start") Instant start,
      @JsonProperty("end") Instant end,
      @JsonProperty("component") String component,
      @JsonProperty("workflow") String workflow,
      @JsonProperty("concurrency") long concurrency,
      @JsonProperty("docker_image") Optional<String> docker_image,
      @JsonProperty("processed") Boolean processed,
      @JsonProperty("resource") String resource) {
    final String resolvedId =
        id != null ? id : RandomGenerator.DEFAULT.generateUniqueId("backfill");
    final boolean resolvedProcessed = processed != null ? processed : false;
    final String resolvedResource = resource != null ? resource : resolvedId;
    return new AutoValue_Backfill(resolvedId, start, end, component, workflow, concurrency, docker_image,
        resolvedProcessed, resolvedResource);
  }
}
