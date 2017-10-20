/*-
 * -\-\-
 * Spotify Styx Common
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

package com.spotify.styx.state;

import com.spotify.styx.util.IsClosed;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single threaded asynchronous event consumer queue. It requires a {@link Consumer}
 * implementation to act upon the queued events of type {@link T}.
 */
public class EventFeeder<T> {

  private static final Logger LOG = LoggerFactory.getLogger(EventFeeder.class);

  private final Consumer<T> eventConsumer;
  private final ExecutorService executor;

  public EventFeeder(Consumer<T> eventConsumer, ExecutorService executor) {
    this.eventConsumer = Objects.requireNonNull(eventConsumer);
    this.executor = Objects.requireNonNull(executor);
  }

  void enqueue(T event) throws IsClosed {
    if (executor.isShutdown()) {
      throw new IsClosed();
    }
    try {
      executor.execute(() -> eventConsumer.accept(event));
    } catch (Exception e) {
      LOG.warn("Exception while enqueuing event {}: {}", event, e);
    }
  }
}
