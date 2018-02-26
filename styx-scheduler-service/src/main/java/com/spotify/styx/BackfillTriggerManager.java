/*-
 * -\-\-
 * Spotify Styx Scheduler Service
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

package com.spotify.styx;

import static com.google.common.base.CaseFormat.LOWER_UNDERSCORE;
import static com.google.common.base.CaseFormat.UPPER_CAMEL;
import static com.spotify.styx.util.GuardedRunnable.guard;
import static com.spotify.styx.util.TimeUtil.nextInstant;
import static com.spotify.styx.util.TimeUtil.numberOfInstants;

import com.google.common.annotations.VisibleForTesting;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.storage.StorageTransaction;
import com.spotify.styx.util.AlreadyInitializedException;
import com.spotify.styx.util.Time;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Triggers backfill executions for {@link Workflow}s.
 */
class BackfillTriggerManager {

  private static final Logger LOG = LoggerFactory.getLogger(BackfillTriggerManager.class);

  private static final String TICK_TYPE = UPPER_CAMEL.to(
      LOWER_UNDERSCORE, BackfillTriggerManager.class.getSimpleName());

  static Consumer<List<Backfill>> DEFAULT_SHUFFLER = Collections::shuffle;

  private final TriggerListener triggerListener;
  private final Storage storage;
  private final StateManager stateManager;
  private final WorkflowCache workflowCache;
  private final Stats stats;
  private final Time time;
  private final Consumer<List<Backfill>> shuffler;

  BackfillTriggerManager(StateManager stateManager,
                         WorkflowCache workflowCache, Storage storage,
                         TriggerListener triggerListener,
                         Stats stats,
                         Time time) {
    this(stateManager, workflowCache, storage, triggerListener, stats, time, DEFAULT_SHUFFLER);
  }

  @VisibleForTesting
  BackfillTriggerManager(StateManager stateManager,
                         WorkflowCache workflowCache, Storage storage,
                         TriggerListener triggerListener,
                         Stats stats,
                         Time time,
                         Consumer<List<Backfill>> shuffler) {
    this.stateManager = Objects.requireNonNull(stateManager);
    this.workflowCache = Objects.requireNonNull(workflowCache);
    this.storage = Objects.requireNonNull(storage);
    this.triggerListener = Objects.requireNonNull(triggerListener);
    this.stats = Objects.requireNonNull(stats);
    this.time = Objects.requireNonNull(time);
    this.shuffler = Objects.requireNonNull(shuffler);
  }

  void tick() {
    LOG.debug("ticking backfill trigger manager");
    final Instant t0 = time.get();

    final List<Backfill> immutableBackfillList;
    try {
      immutableBackfillList = storage.backfills(false);
    } catch (IOException e) {
      LOG.warn("Failed to get backfills", e);
      return;
    }

    final List<Backfill> backfills = new ArrayList<>(immutableBackfillList);
    shuffler.accept(backfills);

    LOG.debug("Found backfill number {}", backfills.size());
    backfills.forEach(backfill -> guard(() -> triggerAndProgress(backfill)).run());

    final long durationMillis = t0.until(time.get(), ChronoUnit.MILLIS);
    stats.recordTickDuration(TICK_TYPE, durationMillis);
  }

  private void triggerAndProgress(Backfill backfill) {
    final Optional<Workflow> workflowOpt = workflowCache.workflow(backfill.workflowId());
    if (!workflowOpt.isPresent()) {
      LOG.debug("workflow not found for backfill {}, halt it.", backfill);
      storeBackfill(backfill.builder().halted(true).build());
      return;
    }
    LOG.debug("Workflow present for backfill {}", workflowOpt.get());
    final Workflow workflow = workflowOpt.get();

    // this is best effort because querying active states is not strongly consistent so the
    // initial remaining capacity may already be wrong
    final int remainingCapacity =
        backfill.concurrency() - stateManager.activeStatesByTriggerId(backfill.id()).size();

    LOG.debug("Remaining capacity for backfill {}", backfill);
    if (remainingCapacity < 1) {
      LOG.debug("No capacity left for backfill {}", backfill);
      return;
    }

    final Instant initialNextTrigger = backfill.nextTrigger();

    Instant nextTrigger;
    do {
      try {
        LOG.debug("loopoing backfill {}, initialNT={}, remainingCapacity={}",
            backfill, initialNextTrigger, remainingCapacity);
        nextTrigger = storage.runInTransaction(
            tx -> triggerNextPartitionAndProgress(tx, backfill.id(), workflow,
                                                  initialNextTrigger, remainingCapacity));
      } catch (IOException e) {
        // transaction failure, yield
        LOG.debug("transaction failure when working on backfill {}", backfill, e);
        break;
      }
    } while (nextTrigger != null
             && numberOfInstants(nextTrigger, initialNextTrigger,
                                 backfill.schedule()) < remainingCapacity);
  }

  private Instant triggerNextPartitionAndProgress(StorageTransaction tx,
                                                  String id,
                                                  Workflow workflow,
                                                  Instant initialNextTrigger,
                                                  int remainingCapacity) {
    final Backfill momentBackfill = tx.backfill(id).orElseThrow(RuntimeException::new);

    final Instant partition = momentBackfill.nextTrigger();

    LOG.debug("trigger and progress backfill for workflow {}", workflow);

    // best effort to prevent the case that after this scheduler instance reading
    // initialNextTrigger, the other scheduler instance managed to progress nextTrigger
    if (numberOfInstants(partition, initialNextTrigger,
                     momentBackfill.schedule()) >= remainingCapacity) {
      return null;
    }

    // this is to prevent the other scheduler instance re-entering
    if (partition.equals(momentBackfill.end())) {
      LOG.debug("backfill {} all triggered", momentBackfill);
      return null;
    }

    final Instant nextPartition;

    try {
      // worse case:
      //   - the other scheduler instance has triggered partition, but hasn't committed the
      //     transaction, and the workflow instance has finished very quickly
      //   - this scheduler reads initialNextTrigger before the other scheduler commits the
      //     transaction, so it will move on to trigger the same partition again, but we still
      //     don't violate concurrency limit in this case
      final CompletableFuture<Void> processed = triggerListener
          .event(workflow, Trigger.backfill(momentBackfill.id()), partition)
          .toCompletableFuture();
      // Wait for the trigger execution to complete before proceeding to the next partition
      processed.get();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof AlreadyInitializedException) {
        // just encountered an ad-hoc trigger or it has been triggered by another scheduler instance
        // do not propagate the exception but instead letting transaction decide what to do
        LOG.debug("{} already triggered for backfill {}", partition, momentBackfill, e);
      } else {
        LOG.debug("failed to trigger {} for backfill {}", partition, momentBackfill, e);
        throw new RuntimeException(e);
      }
    } catch (Exception e) {
      // whatever, can be interrupted
      // we give up this backfill and retry in next tick
      LOG.debug("failed to trigger {} for backfill {}", partition, momentBackfill, e);
      throw new RuntimeException(e);
    }

    LOG.debug("updating backfill nextTrigger for {}", momentBackfill);

    nextPartition = nextInstant(partition, momentBackfill.schedule());

    tx.store(momentBackfill.builder()
                      .nextTrigger(nextPartition)
                      .build());

    if (nextPartition.equals(momentBackfill.end())) {
      tx.store(momentBackfill.builder()
                           .nextTrigger(momentBackfill.end())
                           .allTriggered(true)
                           .build());
      LOG.debug("backfill {} all triggered", momentBackfill);
      return null;
    }

    return nextPartition;
  }

  private void storeBackfill(Backfill backfill) {
    try {
      storage.storeBackfill(backfill);
    } catch (IOException e) {
      LOG.warn("Failed to store updated backfill", e);
    }
  }
}
