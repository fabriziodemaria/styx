/*-
 * -\-\-
 * styx-client
 * --
 * Copyright (C) 2016 - 2017 Spotify AB
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

package com.spotify.styx.client;

import static com.spotify.styx.serialization.Json.OBJECT_MAPPER;
import static com.spotify.styx.serialization.Json.serialize;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.spotify.apollo.Client;
import com.spotify.apollo.Request;
import com.spotify.apollo.Response;
import com.spotify.futures.CompletableFutures;
import com.spotify.styx.api.BackfillPayload;
import com.spotify.styx.api.BackfillsPayload;
import com.spotify.styx.api.ResourcesPayload;
import com.spotify.styx.api.RunStateDataPayload;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.BackfillInput;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.Resource;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowState;
import com.spotify.styx.model.data.EventInfo;
import com.spotify.styx.util.EventUtil;
import com.squareup.okhttp.HttpUrl;
import com.squareup.okhttp.HttpUrl.Builder;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import javaslang.Tuple;
import javaslang.Tuple2;
import okio.ByteString;

/**
 * Styx Apollo Client Implementation. In case of API errors, the {@link Throwable} in the returned
 * {@link CompletionStage} will be of kind {@link ApiErrorException}. Other errors will be treated
 * as {@link RuntimeException} instead.
 */
class StyxApolloClient implements StyxClient {
  private static final String STYX_API_ENDPOINT = "/api/v2";
  private static final String UTF_8 = "UTF-8";
  private static final String STYX_CLIENT_VERSION =
      "Styx Client " + StyxApolloClient.class.getPackage().getImplementationVersion();
  private static final int TTL_SECONDS = 90;

  private final String apiHost;
  private final Client client;

  StyxApolloClient(final Client client,
                   final String apiHost) {
    this.apiHost = apiHost;
    this.client = client;
  }

  @Override
  public CompletionStage<RunStateDataPayload> activeStates(Optional<String> componentId) {
    String url = apiUrl("status", "activeStates");
    if (componentId.isPresent()) {
      url = addQueryToApiUrl(url, ImmutableList.of(Tuple.of("component", componentId.get())));
    }
    return executeRequest(
        Request.forUri(url).withTtl(Duration.ofSeconds(TTL_SECONDS)),
        RunStateDataPayload.class);
  }

  @Override
  public CompletionStage<List<EventInfo>> eventsForWorkflowInstance(String componentId,
                                                                    String workflowId,
                                                                    String parameter) {
    final String url = apiUrl("status", "events", componentId, workflowId, parameter);
    return executeRequest(Request.forUri(url).withTtl(Duration.ofSeconds(TTL_SECONDS)))
        .thenApply(response -> {
          final JsonNode jsonNode;
          try {
            if (!response.payload().isPresent()) {
              throw new RuntimeException("No json returned from API");
            }
            jsonNode = OBJECT_MAPPER.readTree(response.payload().get().toByteArray());
          } catch (IOException e) {
            throw new RuntimeException("Invalid json returned from API");
          }

          if (!jsonNode.isObject()) {
            throw new RuntimeException("Unexpected json returned from API");
          }

          final ObjectNode json = (ObjectNode) jsonNode;
          final ArrayNode events = json.withArray("events");
          final ImmutableList.Builder<EventInfo> eventInfos = ImmutableList.builder();
          for (JsonNode eventWithTimestamp : events) {
            final long ts = eventWithTimestamp.get("timestamp").asLong();
            final JsonNode event = eventWithTimestamp.get("event");

            String eventName;
            String eventInfo;
            try {
              Event typedEvent = OBJECT_MAPPER.convertValue(event, Event.class);
              eventName = EventUtil.name(typedEvent);
              eventInfo = EventUtil.info(typedEvent);
            } catch (IllegalArgumentException e) {
              // fall back to just inspecting the json
              eventName = event.get("@type").asText();
              eventInfo = "";
            }

            eventInfos.add(EventInfo.create(ts, eventName, eventInfo));
          }
          return eventInfos.build();
        });
  }

  @Override
  public CompletionStage<Workflow> workflow(String componentId, String workflowId) {
    final String url = apiUrl("workflows", componentId, workflowId);
    return executeRequest(Request.forUri(url), Workflow.class);
  }

  @Override
  public CompletionStage<WorkflowState> workflowState(String componentId, String workflowId) {
    final String url = apiUrl("workflows", componentId, workflowId, "state");
    return executeRequest(Request.forUri(url), WorkflowState.class);
  }

  @Override
  public CompletionStage<Void> triggerWorkflowInstance(String componentId,
                                                       String workflowId,
                                                       String parameter) {
    final String url = apiUrl("scheduler", "trigger", componentId, workflowId, parameter);
    return executeRequest(Request.forUri(url, "POST")).thenApply(response -> (Void) null);
  }

  @Override
  public CompletionStage<Void> haltWorkflowInstance(String componentId,
                                                    String workflowId,
                                                    String parameter) {
    final String url = apiUrl("scheduler", "halt", componentId, workflowId, parameter);
    return executeRequest(Request.forUri(url, "POST")).thenApply(response -> (Void) null);
  }

  @Override
  public CompletionStage<Void> retryWorkflowInstance(String componentId,
                                                     String workflowId,
                                                     String parameter) {
    final String url = apiUrl("scheduler", "retry", componentId, workflowId, parameter);
    return executeRequest(Request.forUri(url, "POST")).thenApply(response -> (Void) null);
  }

  @Override
  public CompletionStage<Resource> resourceCreate(String resourceId, int concurrency) {
    final String url = apiUrl("resources");
    try {
      final ByteString payload = serialize(Resource.create(resourceId, concurrency));
      return executeRequest(Request.forUri(url, "POST").withPayload(payload), Resource.class);
    } catch (JsonProcessingException e) {
      return CompletableFutures.exceptionallyCompletedFuture(new RuntimeException(e));
    }
  }

  @Override
  public CompletionStage<Resource> resourceEdit(String resourceId, int concurrency) {
    final String url = apiUrl("resources", resourceId);
    try {
      final ByteString payload = serialize(Resource.create(resourceId, concurrency));
      return executeRequest(Request.forUri(url, "PUT").withPayload(payload), Resource.class);
    } catch (JsonProcessingException e) {
      return CompletableFutures.exceptionallyCompletedFuture(new RuntimeException(e));
    }
  }

  @Override
  public CompletionStage<Resource> resource(String resourceId) {
    final String url = apiUrl("resources", resourceId);
    return executeRequest(Request.forUri(url), Resource.class);
  }

  @Override
  public CompletionStage<ResourcesPayload> resourceList() {
    final String url = apiUrl("resources");
    return executeRequest(Request.forUri(url), ResourcesPayload.class);
  }

  @Override
  public CompletionStage<Backfill> backfillCreate(String componentId, String workflowId,
                                                  String start, String end,
                                                  int concurrency) {
    final String url = apiUrl("backfills");
    try {
      final ByteString payload = serialize(BackfillInput.create(
          Instant.parse(start), Instant.parse(end), componentId, workflowId, concurrency));
      return executeRequest(Request.forUri(url, "POST").withPayload(payload), Backfill.class);
    } catch (JsonProcessingException e) {
      return CompletableFutures.exceptionallyCompletedFuture(new RuntimeException(e));
    }
  }

  @Override
  public CompletionStage<Backfill> backfillEditConcurrency(String backfillId, int concurrency) {
    return backfill(backfillId).thenCompose(backfillPayload -> {
      final Backfill editedBackfill = backfillPayload.backfill().builder().concurrency(concurrency).build();
      final String url = apiUrl("backfills", backfillId);
      try {
        final ByteString payload = serialize(editedBackfill);
        return executeRequest(Request.forUri(url, "PUT").withPayload(payload), Backfill.class);
      } catch (JsonProcessingException e) {
        return CompletableFutures.exceptionallyCompletedFuture(new RuntimeException(e));
      }
    });
  }

  @Override
  public CompletionStage<Void> backfillHalt(String backfillId) {
    final String url = apiUrl("backfills");
    return executeRequest(Request.forUri(url, "DELETE")).thenApply(response -> (Void) null);
  }

  @Override
  public CompletionStage<BackfillPayload> backfill(String backfillId) {
    final String url = apiUrl("backfills", backfillId);
    return executeRequest(Request.forUri(url), BackfillPayload.class);
  }

  @Override
  public CompletionStage<BackfillsPayload> backfillList(Optional<String> componentId,
                                                        Optional<String> workflowId,
                                                        boolean showAll,
                                                        boolean status) {
    final List<Tuple2<String, String>> queries = new ArrayList<>();
    componentId.ifPresent(c -> queries.add(Tuple.of("component", c)));
    workflowId.ifPresent(w -> queries.add(Tuple.of("workflow", w)));
    queries.add(Tuple.of("showAll", Boolean.toString(showAll)));
    queries.add(Tuple.of("status", Boolean.toString(status)));

    String url = apiUrl("backfills");
    url = addQueryToApiUrl(url, queries);

    return executeRequest(Request.forUri(url), BackfillsPayload.class);
  }

  private <T> CompletionStage<T> executeRequest(final Request request,
                                                final Class<T> tClass) {
    return executeRequest(request).thenApply(response -> {
      if (!response.payload().isPresent()) {
        throw new RuntimeException("Expected payload not found");
      } else {
        try {
          return OBJECT_MAPPER.readValue(response.payload().get().toByteArray(), tClass);
        } catch (IOException e) {
          throw new RuntimeException("Error while reading the received payload: " + e);
        }
      }
    });
  }

  private CompletionStage<Response<ByteString>> executeRequest(final Request request) {
    return client.send(request.withHeader("User-Agent", STYX_CLIENT_VERSION)).thenApply(response -> {
      switch (response.status().family()) {
        case SUCCESSFUL:
          return response;
        default:
          final String message = response.status().code() + " " + response.status().reasonPhrase();
          throw new ApiErrorException(message, response.status().code());
      }
    });
  }

  private String apiUrl(String... pathSegments) {
    final Builder urlBuilder = new HttpUrl.Builder()
        .scheme("http")
        .host(apiHost);

    for (String pathSegment : pathSegments) {
      urlBuilder.addPathSegment(pathSegment);
    }
    return urlBuilder.build().toString();
  }

  private String addQueryToApiUrl(String url, List<Tuple2<String, String>> queries) {
    final Builder urlBuilder = HttpUrl.parse(url).newBuilder();

    for (Tuple2<String, String> queryTuple : queries) {
      urlBuilder.addQueryParameter(queryTuple._1, queryTuple._2);
    }
    return urlBuilder.build().toString();
  }
}
