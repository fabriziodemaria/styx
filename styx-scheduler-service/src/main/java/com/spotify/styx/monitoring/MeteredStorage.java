/*-
 * -\-\-
 * Spotify Styx Scheduler Service
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

package com.spotify.styx.monitoring;

import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.Resource;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.model.WorkflowState;
import com.spotify.styx.model.data.WorkflowInstanceExecutionData;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.util.Time;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;

public final class MeteredStorage extends MeteredBase implements Storage {

  private final Storage delegate;

  public MeteredStorage(Storage delegate, Stats stats, Time time) {
    super(stats, time);
    this.delegate = Objects.requireNonNull(delegate);
  }

  @Override
  public SortedSet<SequenceEvent> readEvents(WorkflowInstance workflowInstance) throws IOException {
    return timedStorage("readEvents", () -> delegate.readEvents(workflowInstance));
  }

  @Override
  public void writeEvent(SequenceEvent sequenceEvent) throws IOException {
    timedStorage("writeEvent", () -> delegate.writeEvent(sequenceEvent));
  }

  @Override
  public Optional<Long> getLatestStoredCounter(WorkflowInstance workflowInstance)
      throws IOException {
    return timedStorage("getLatestStoredCounter", () -> delegate.getLatestStoredCounter(workflowInstance));
  }

  @Override
  public boolean globalEnabled() throws IOException {
    return timedStorage("globalEnabled", delegate::globalEnabled);
  }

  @Override
  public boolean setGlobalEnabled(boolean enabled) throws IOException {
    return timedStorage("setGlobalEnabled", () -> delegate.setGlobalEnabled(enabled));
  }

  @Override
  public String globalDockerRunnerId() throws IOException {
    return timedStorage("globalDockerRunnerId", delegate::globalDockerRunnerId);
  }

  @Override
  public void store(Workflow workflow) throws IOException {
    timedStorage("storeWorkflow", () -> delegate.store(workflow));
  }

  @Override
  public Optional<Workflow> workflow(WorkflowId workflowId) throws IOException {
    return timedStorage("workflow", () -> delegate.workflow(workflowId));
  }

  @Override
  public void delete(WorkflowId workflowId) throws IOException {
    timedStorage("delete", () -> delegate.delete(workflowId));
  }

  @Override
  public void updateNextNaturalTrigger(WorkflowId workflowId, Instant nextNaturalTrigger) throws IOException {
    timedStorage("updateNextNaturalTrigger",
        () -> delegate.updateNextNaturalTrigger(workflowId, nextNaturalTrigger));
  }

  @Override
  public Map<Workflow, Optional<Instant>> workflowsWithNextNaturalTrigger()
      throws IOException {
    return timedStorage("workflowsWithNextNaturalTrigger",
        () -> delegate.workflowsWithNextNaturalTrigger());
  }

  @Override
  public void writeActiveState(WorkflowInstance workflowInstance, long counter) throws IOException {
    timedStorage("writeActiveState", () -> delegate.writeActiveState(workflowInstance, counter));
  }

  @Override
  public void deleteActiveState(WorkflowInstance workflowInstance) throws IOException {
    timedStorage("deleteActiveState", () -> delegate.deleteActiveState(workflowInstance));
  }

  @Override
  public Map<WorkflowInstance, Long> readActiveWorkflowInstances() throws IOException {
    return timedStorage("readActiveWorkflowInstances", () -> delegate.readActiveWorkflowInstances());
  }

  @Override
  public Map<WorkflowInstance, Long> readActiveWorkflowInstances(String componentId) throws IOException {
    return timedStorage("readActiveWorkflowInstances", () -> delegate.readActiveWorkflowInstances(componentId));
  }

  @Override
  public WorkflowInstanceExecutionData executionData(WorkflowInstance workflowInstance) throws IOException {
    return timedStorage("executionData", () -> delegate.executionData(workflowInstance));
  }

  @Override
  public List<WorkflowInstanceExecutionData> executionData(WorkflowId workflowId, String offset,
                                                           int limit) throws IOException {

    return timedStorage("executionData", () -> delegate.executionData(workflowId, offset, limit));
  }

  @Override
  public boolean enabled(WorkflowId workflowId) throws IOException {
    return timedStorage("enabled", () -> delegate.enabled(workflowId));
  }

  @Override
  public Set<WorkflowId> enabled() throws IOException {
    return timedStorage("enabled", () -> delegate.enabled());
  }

  @Override
  public void patchState(WorkflowId workflowId, WorkflowState state) throws IOException {
    timedStorage("patchState", () -> delegate.patchState(workflowId, state));
  }

  @Override
  public void patchState(String componentId, WorkflowState state) throws IOException {
    timedStorage("patchState", () -> delegate.patchState(componentId, state));
  }

  @Override
  public Optional<String> getDockerImage(WorkflowId workflowId) throws IOException {
    return timedStorage("getDockerImage", () -> delegate.getDockerImage(workflowId));
  }

  @Override
  public WorkflowState workflowState(WorkflowId workflowId) throws IOException {
    return timedStorage("workflowState", () -> delegate.workflowState(workflowId));
  }

  @Override
  public Optional<Resource> resource(String id) throws IOException {
    return timedStorage("resource", () -> delegate.resource(id));
  }

  @Override
  public void storeResource(Resource resource) throws IOException {
    timedStorage("storeResource", () -> delegate.storeResource(resource));
  }

  @Override
  public List<Resource> resources() throws IOException {
    return timedStorage("resources", delegate::resources);
  }

  @Override
  public void deleteResource(String id) throws IOException {
    timedStorage("deleteResource", () -> delegate.deleteResource(id));
  }

  @Override
  public List<Backfill> backfills() throws IOException {
    return timedStorage("backfills", delegate::backfills);
  }

  @Override
  public Optional<Backfill> backfill(String id) throws IOException {
    return timedStorage("backfill", () -> delegate.backfill(id));
  }

  @Override
  public void storeBackfill(Backfill backfill) throws IOException {
    timedStorage("storeBackfill", () -> delegate.storeBackfill(backfill));
  }
}
