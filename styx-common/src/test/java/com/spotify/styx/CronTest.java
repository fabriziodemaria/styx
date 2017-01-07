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

package com.spotify.styx;

import com.cronutils.descriptor.CronDescriptor;
import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinition;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Locale;
import org.junit.Test;

public class CronTest {

  /**
   * - hourly  - 2016-04-01T14         `0 * * * *`
   * - daily   - 2016-04-01            `0 0 * * *`
   * - weekly  - 2016-04-04 (Mondays)  `0 0 * * MON`
   * - monthly - 2016-04-01            `0 0 1 * *`
   * - yearly  - 2016-01-01            `0 0 1 1 *`
   *
   * impl plan
   * - update Partitioning enum to class containing cron def
   *   - convert existing values to equiv. cron def
   * - update mapping to workflow instance parameters
   *   - StateInitializingTrigger.toParameter
   * - api
   *   - update trigger endpoint, workflow instance verification
   * - TriggerManager
   *   - trigger (decrement instant)
   *   - storage.updateNextNaturalTrigger
   * - StyxScheduler
   *   - workflowChanged, calculate next aligned natural trigger time
   *   - storage.patchState
   * - Stay compatible with UI
   *   - 'partitioning' field in api is 'hours', 'days', etc
   */

  @Test
  public void name() throws Exception {
    print("hourly", "0 * * * *");
    print("daily", "0 0 * * *");
    print("weekly", "0 0 * * MON");
    print("monthly", "0 0 1 * *");
    print("yearly", "0 0 1 1 *");

    print("custom", "*/5 0,12 * * MON,WED,FRI");
  }

  private void print(String name, String line) {
    System.out.println("=== " + name + " ===");
    System.out.println(line);

    final CronDefinition cronDefinition =
        CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX);

    CronParser parser = new CronParser(cronDefinition);
    Cron cron = parser.parse(line);

    // Create a descriptor for a specific Locale
    CronDescriptor descriptor = CronDescriptor.instance(Locale.US);
    String description = descriptor.describe(cron);
    System.out.println("description = " + description);

    ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
    ExecutionTime executionTime = ExecutionTime.forCron(cron);

    ZonedDateTime lastExecution = executionTime.lastExecution(now);
    System.out.println("lastExecution     = " + lastExecution);
    // Get date for next execution
    ZonedDateTime nextExecution = executionTime.nextExecution(now);
    System.out.println("nextExecution     = " + nextExecution);
    // Get date for next execution after that
    ZonedDateTime nextNextExecution = executionTime.nextExecution(nextExecution);
    System.out.println("nextNextExecution = " + nextNextExecution);
  }
}
