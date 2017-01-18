/*-
 * -\-\-
 * Spotify Styx API Service
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

package com.spotify.styx.api;

import static com.spotify.styx.api.Api.Version.V0;
import static com.spotify.styx.api.Api.Version.V1;
import static com.spotify.styx.model.EventSerializer.convertEventToPersistentEvent;
import static com.spotify.styx.util.ReplayEvents.replayActiveStates;
import static com.spotify.styx.util.StreamUtil.cat;
import static java.util.stream.Collectors.toList;

import com.google.api.client.util.Lists;
import com.google.common.base.Throwables;
import com.spotify.apollo.RequestContext;
import com.spotify.apollo.Response;
import com.spotify.apollo.entity.EntityMiddleware;
import com.spotify.apollo.entity.JacksonEntityCodec;
import com.spotify.apollo.route.AsyncHandler;
import com.spotify.apollo.route.Middleware;
import com.spotify.apollo.route.Route;
import com.spotify.styx.api.cli.ActiveStatesPayload;
import com.spotify.styx.api.cli.EventsPayload;
import com.spotify.styx.api.cli.EventsPayload.TimestampedPersistentEvent;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState;
import com.spotify.styx.storage.Storage;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;
import okio.ByteString;

/**
 * API endpoints for the cli
 */
public class CliResource {

  public static final String BASE = "/cli";
  public static final String SCHEDULER_BASE_PATH = "/api/v0";

  private final String schedulerServiceBaseUrl;
  private final Storage storage;

  public CliResource(String schedulerServiceBaseUrl, Storage storage) {
    this.schedulerServiceBaseUrl = Objects.requireNonNull(schedulerServiceBaseUrl);
    this.storage = Objects.requireNonNull(storage);
  }

  public Stream<? extends Route<? extends AsyncHandler<? extends Response<ByteString>>>> routes() {
    final EntityMiddleware em =
        EntityMiddleware.forCodec(JacksonEntityCodec.forMapper(Middlewares.OBJECT_MAPPER));

    final List<Route<AsyncHandler<Response<ByteString>>>> routes = Stream.of(
        Route.with(
            em.serializerDirect(ActiveStatesPayload.class),
            "GET", BASE + "/activeStates",
            this::activeStates),
        Route.with(
            em.serializerDirect(EventsPayload.class),
            "GET", BASE + "/events/<cid>/<eid>/<iid>",
            rc -> eventsForWorkflowInstance(arg("cid", rc), arg("eid", rc), arg("iid", rc))))

        .map(r -> r.withMiddleware(Middleware::syncToAsync))
        .collect(toList());

    final List<Route<AsyncHandler<Response<ByteString>>>> schedulerProxies = Arrays.asList(
        Route.async(
            "GET", BASE + "/<endpoint:path>",
            rc -> proxyToScheduler("/" + arg("endpoint", rc), rc)),
        Route.async(
            "POST", BASE + "/<endpoint:path>",
            rc -> proxyToScheduler("/" + arg("endpoint", rc), rc)),
        Route.async(
            "DELETE", BASE + "/<endpoint:path>",
            rc -> proxyToScheduler("/" + arg("endpoint", rc), rc)),
        Route.async(
            "PATCH", BASE + "/<endpoint:path>",
            rc -> proxyToScheduler("/" + arg("endpoint", rc), rc)),
        Route.async(
            "PUT", BASE + "/<endpoint:path>",
            rc -> proxyToScheduler("/" + arg("endpoint", rc), rc))
    );

    return cat(
        Api.prefixRoutes(routes, V0, V1),
        Api.prefixRoutes(schedulerProxies, V0, V1)
    );
  }

  private CompletionStage<Response<ByteString>> proxyToScheduler(String path, RequestContext rc) {
    return rc.requestScopedClient()
        .send(rc.request().withUri(schedulerServiceBaseUrl + SCHEDULER_BASE_PATH + path));
  }

  private static String arg(String name, RequestContext rc) {
    return rc.pathArgs().get(name);
  }

  private ActiveStatesPayload activeStates(RequestContext requestContext) {
    final Optional<String> componentOpt = requestContext.request().parameter("component");

    final List<ActiveStatesPayload.ActiveState> runStates = Lists.newArrayList();
    try {

      final Map<WorkflowInstance, Long> activeStates = componentOpt.isPresent()
          ? storage.readActiveWorkflowInstances(componentOpt.get())
          : storage.readActiveWorkflowInstances();

      final Map<RunState, Long> map = replayActiveStates(activeStates, storage, false);
      runStates.addAll(
          map.keySet().stream().map(this::runStateToActiveState).collect(toList()));
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    return ActiveStatesPayload.create(runStates);
  }

  private ActiveStatesPayload.ActiveState runStateToActiveState(RunState state) {
    return ActiveStatesPayload.ActiveState.create(
        state.workflowInstance(),
        state.state().toString(),
        state.data()
    );
  }

  private EventsPayload eventsForWorkflowInstance(String cid, String eid, String iid) {
    final WorkflowId workflowId = WorkflowId.create(cid, eid);
    final WorkflowInstance workflowInstance = WorkflowInstance.create(workflowId, iid);

    try {
      final Set<SequenceEvent> sequenceEvents = storage.readEvents(workflowInstance);
      final List<TimestampedPersistentEvent> timestampedPersistentEvents = sequenceEvents.stream()
          .map(sequenceEvent -> TimestampedPersistentEvent.create(
              convertEventToPersistentEvent(sequenceEvent.event()),
              sequenceEvent.timestamp()))
          .collect(toList());

      return EventsPayload.create(timestampedPersistentEvents);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
