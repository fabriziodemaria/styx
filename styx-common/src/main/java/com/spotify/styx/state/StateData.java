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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.spotify.styx.model.ExecutionDescription;
import java.util.Optional;

/**
 * A value type for holding data related to the various states of the {@link RunState}.
 */
@AutoValue
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class StateData {

  @JsonProperty
  public abstract int tries();

  @JsonProperty
  public abstract double retryCost();

  @JsonProperty
  public abstract Optional<Long> retryDelayMillis();

  @JsonProperty
  public abstract Optional<Integer> lastExit();

  @JsonProperty
  public abstract Optional<String> trigger();

  @JsonProperty
  public abstract Optional<String> executionId();

  @JsonProperty
  public abstract Optional<ExecutionDescription> executionDescription();

  @JsonProperty
  public abstract ImmutableList<Message> messages();

  public static Builder builder() {
    return new AutoValue_StateData.Builder()
        .tries(0)
        .retryCost(0.0);
  }

  public abstract Builder toBuilder();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder tries(int value);
    public abstract Builder retryCost(double value);
    public abstract Builder retryDelayMillis(Long value);
    public abstract Builder lastExit(Optional<Integer> value);
    public abstract Builder lastExit(Integer value);
    public abstract Builder trigger(String value);
    public abstract Builder executionId(String value);
    public abstract Builder executionDescription(ExecutionDescription value);
    abstract ImmutableList.Builder<Message> messagesBuilder();
    public Builder addMessage(Message value) {
      messagesBuilder().add(value);
      return this;
    }

    public abstract StateData build();
  }

}
