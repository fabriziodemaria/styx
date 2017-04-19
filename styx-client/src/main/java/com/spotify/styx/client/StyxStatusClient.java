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

import com.spotify.styx.api.RunStateDataPayload;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.model.data.EventInfo;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * Interface for Styx client, status resources.
 */
public interface StyxStatusClient {

  /**
   * Get information about the active stats
   *
   * @param componentId component id to filter on
   * @return The information about the active states
   */
  CompletionStage<RunStateDataPayload> activeStates(final Optional<String> componentId);

  /**
   * Get {@link Event}s issued for a {@link WorkflowInstance}
   *
   * @param componentId component id
   * @param workflowId  workflow id
   * @param parameter   parameter
   *
   * @return The list of {@link Event}s for the selected {@link WorkflowInstance}
   */
  CompletionStage<List<EventInfo>> eventsForWorkflowInstance(final String componentId,
                                                             final String workflowId,
                                                             final String parameter);
}
