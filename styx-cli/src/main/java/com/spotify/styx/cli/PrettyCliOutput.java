/*-
 * -\-\-
 * Spotify Styx CLI
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

package com.spotify.styx.cli;

import static com.spotify.styx.cli.CliUtil.colored;
import static com.spotify.styx.cli.CliUtil.coloredBright;
import static com.spotify.styx.cli.CliUtil.formatTimestamp;
import static org.fusesource.jansi.Ansi.Color.BLACK;
import static org.fusesource.jansi.Ansi.Color.BLUE;
import static org.fusesource.jansi.Ansi.Color.CYAN;
import static org.fusesource.jansi.Ansi.Color.DEFAULT;
import static org.fusesource.jansi.Ansi.Color.GREEN;
import static org.fusesource.jansi.Ansi.Color.RED;
import static org.fusesource.jansi.Ansi.Color.YELLOW;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.spotify.styx.api.BackfillPayload;
import com.spotify.styx.api.cli.RunStateDataPayload;
import com.spotify.styx.api.cli.RunStateDataPayload.RunStateData;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.Partitioning;
import com.spotify.styx.serialization.Json;
import com.spotify.styx.state.Message;
import com.spotify.styx.state.RunState.State;
import com.spotify.styx.state.StateData;
import com.spotify.styx.util.ParameterUtil;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.P;
import org.fusesource.jansi.Ansi;
import org.fusesource.jansi.Ansi.Color;

class PrettyCliOutput implements CliOutput {

  @Override
  public void printStates(RunStateDataPayload runStateDataPayload) {
    System.out.println(String.format(
        "  %-20s %-12s %-47s %-7s %s",
        "WORKFLOW INSTANCE",
        "STATE",
        "LAST EXECUTION ID",
        "TRIES",
        "PREVIOUS EXECUTION MESSAGE"));

    CliUtil.groupStates(runStateDataPayload.activeStates()).entrySet().forEach(entry -> {
      System.out.println();
      System.out.println(String.format("%s %s",
                                       colored(CYAN, entry.getKey().componentId()),
                                       colored(BLUE, entry.getKey().endpointId())));
      entry.getValue().forEach(RunStateData -> {
        final StateData stateData = RunStateData.stateData();
        final List<Message> messages = stateData.messages();

        final Ansi lastMessage;
        if (messages.isEmpty()) {
          lastMessage = colored(DEFAULT, "No info");
        } else {
          final Message message = messages.get(messages.size() - 1);
          final Ansi.Color messageColor = messageColor(message.level());
          lastMessage = colored(messageColor, message.line());
        }

        final Ansi ansi_state;
        switch (RunStateData.state()) {
          case "QUEUED":
            ansi_state = coloredBright(BLACK, RunStateData.state());
            break;

          case "RUNNING":
            ansi_state = coloredBright(GREEN, RunStateData.state());
            break;

          case "UNKNOWN":
            ansi_state = coloredBright(YELLOW, RunStateData.state());
            break;

          case "WAITING":
            ansi_state = coloredBright(BLACK, RunStateData.state());
            break;

          case "DONE":
            ansi_state = coloredBright(BLUE, RunStateData.state());
            break;

          case "ERROR":
            ansi_state = coloredBright(RED, RunStateData.state());
            break;

          default:
            ansi_state = colored(DEFAULT, RunStateData.state());
        }

        System.out.println(String.format(
            "  %-20s %-20s %-47s %-7d %s",
            RunStateData.workflowInstance().parameter(),
            ansi_state,
            stateData.executionId().orElse("<no-execution-id>"),
            stateData.tries(),
            lastMessage
        ));
      });
    });
  }

  private Ansi.Color messageColor(Message.MessageLevel level) {
    switch (level) {
      case INFO:    return GREEN;
      case WARNING: return YELLOW;
      case ERROR:   return RED;
      default:      return DEFAULT;
    }
  }

  @Override
  public void printEvents(List<EventInfo> eventInfos) {
    final String format = "%-25s %-25s %s";
    System.out.println(String.format(format,
                                     "TIME",
                                     "EVENT",
                                     "DATA"));
    eventInfos.forEach(
        eventInfo ->
            System.out.println(String.format(format,
                                             formatTimestamp(eventInfo.timestamp()),
                                             eventInfo.name(),
                                             eventInfo.info())));
  }

  @Override
  public void printBackfill(Backfill backfill) {
    System.out.println(backfill);
  }

  @Override
  public void printBackfill(BackfillPayload backfillStatus) {
    System.out.println(colored(RED, "\\_ Info"));
    System.out.println(String.format("Backfill id: %s", backfillStatus.backfill().id()));
    System.out.println(String.format("Start instance (inclusive): %s",
        ParameterUtil.toParameter(backfillStatus.backfill().partitioning(), backfillStatus.backfill().start())));
    System.out.println(String.format("End instance (inclusive): %s",
        ParameterUtil.toParameter(backfillStatus.backfill().partitioning(), ParameterUtil.decrementInstant(backfillStatus.backfill().end(), backfillStatus.backfill().partitioning()))));
    System.out.println(String.format("Next instance: %s",
        ParameterUtil.toParameter(backfillStatus.backfill().partitioning(), backfillStatus.backfill().nextTrigger())));
    System.out.println(String.format("Concurrency: %s",
        backfillStatus.backfill().concurrency()));
    System.out.println();
    System.out.println(colored(RED, "\\_ Progress"));
    System.out.println(String.format("TRIGGERING PROGRESS: %.0f%%",
        Math.ceil(calculateBackfillProgress(
            backfillStatus.backfill().start(),
            backfillStatus.backfill().end(),
            backfillStatus.backfill().nextTrigger(),
            backfillStatus.backfill().partitioning()))));
    printBackfillProgressBar(
            backfillStatus.backfill().start(),
            backfillStatus.backfill().end(),
            backfillStatus.backfill().partitioning(),
            backfillStatus.statuses().get().activeStates());
    System.out.println();
    System.out.println(colored(RED, "\\_ Statuses"));
    printStates(backfillStatus.statuses().get());
  }

  private Double calculateBackfillProgress(Instant start, Instant end, Instant current, Partitioning partitioning) {
    final int numberOfInstances = ParameterUtil.rangeOfInstants(start, end, partitioning).size();
    final int currentSequence = ParameterUtil.rangeOfInstants(start, end, partitioning).indexOf(current);
    return ((double)currentSequence/(double)numberOfInstances) * 100;
  }

  private void printBackfillProgressBar(Instant start, Instant end, Partitioning partitioning, List<RunStateData> statues) {
    final int buckets = 100;
    final int numberOfInstances = ParameterUtil.rangeOfInstants(start, end, partitioning).size();

    System.out.print("<");
    if (numberOfInstances < buckets) {
      int bucket_len = Math.floorDiv(buckets, numberOfInstances);
      ParameterUtil.rangeOfInstants(start, end, partitioning).forEach(instant -> {
          String state = statues.stream().filter(runStateData ->
              runStateData.workflowInstance().parameter().equals(
                  ParameterUtil.toParameter(partitioning, instant))).findFirst().get().state();
          printColorState(state, bucket_len);
        });
      if (bucket_len * numberOfInstances < buckets) {
        int leftover = buckets - (bucket_len * numberOfInstances);
        String state = statues.get(statues.size() - 1).state();
        printColorState(state, leftover);
      }
    } else {
      int range_in_bucket = Math.floorDiv(numberOfInstances, buckets);
      int a = 0;
      int b = range_in_bucket;

      while (b <= numberOfInstances) {
        scanAndPrint(statues, a, b);
        a = b;
        b = b + range_in_bucket;
      }

      if (a < numberOfInstances) {
        scanAndPrint(statues, a, numberOfInstances);
      }
    }
    System.out.print(">");
    System.out.println();
  }

  private void printColorState(String state, int loop) {
    IntStream.range(0, loop).forEach(i -> {
      switch (state) {
        case "DONE":
          System.out.print(coloredBright(CYAN, "/"));
          break;
        case "RUNNING":
          System.out.print(coloredBright(GREEN, "/"));
          break;
        case "ERROR":
          System.out.print(coloredBright(RED, "/"));
          break;
        case "UNKNOWN":
          System.out.print(coloredBright(YELLOW, "/"));
          break;
        case "WAITING":
          System.out.print(coloredBright(BLACK, "/"));
          break;
      }
    });
  }

  private void scanAndPrint(List<RunStateData> statues, int a, int b) {
    Color color;
    int gravity = statues.subList(a, b).stream().mapToInt(runStateData -> {
      switch (runStateData.state()) {
        case "DONE":
          return 1;
        case "RUNNING":
          return 2;
        case "UNKNOWN":
          return 3;
        case "ERROR":
          return 4;
        case "WAITING":
          return 5;
        default:
          return 0;
      }
    }).max().getAsInt();

    switch (gravity) {
      case 1:
        color = CYAN;
        break;
      case 2:
        color = GREEN;
        break;
      case 3:
        color = YELLOW;
        break;
      case 4:
        color = RED;
        break;
      case 5:
        color = BLACK;
        break;
      default:
        color = DEFAULT;
    }
    System.out.print(coloredBright(color, "/"));
  }
}
