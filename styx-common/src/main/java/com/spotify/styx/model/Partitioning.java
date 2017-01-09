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

package com.spotify.styx.model;

import static com.cronutils.model.definition.CronDefinitionBuilder.instanceDefinitionFor;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinition;
import com.cronutils.parser.CronParser;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;
import java.util.Locale;
import java.util.Set;

@AutoValue
public abstract class Partitioning {

  public static final String HOURLY_CRON = "0 * * * *";
  public static final String DAILY_CRON = "0 0 * * *";
  public static final String WEEKLY_CRON = "0 0 * * 1";
  public static final String WEEKLY_CRON_ALT = "0 0 * * MON";
  public static final String MONTHLY_CRON = "0 0 1 * *";
  public static final String YEARLY_CRON = "0 0 1 1 *";

  public static final Partitioning HOURS = create(HOURLY_CRON);
  public static final Partitioning DAYS = create(DAILY_CRON);
  public static final Partitioning WEEKS = create(WEEKLY_CRON);
  public static final Partitioning MONTHS = create(MONTHLY_CRON);
  public static final Partitioning YEARS = create(YEARLY_CRON);

  public abstract String expression();
  public abstract Cron cron();

  @Override
  public String toString() {
    switch (expression()) {
      case HOURLY_CRON:
        return "HOURS";
      case DAILY_CRON:
        return "DAYS";
      case WEEKLY_CRON:
      case WEEKLY_CRON_ALT:
        return "WEEKS";
      case MONTHLY_CRON:
        return "MONTHS";
      case YEARLY_CRON:
        return "YEARS";

      default:
        return expression();
    }
  }

  @JsonValue
  public String toJson() {
    return toString().toLowerCase(Locale.US);
  }

  @JsonCreator
  public static Partitioning fromJson(String json) {
    switch (json.toLowerCase(Locale.US)) {
      case "@hourly":
      case "hourly":
      case "hours":
        return HOURS;
      case "@daily":
      case "daily":
      case "days":
        return DAYS;
      case "@weekly":
      case "weekly":
      case "weeks":
        return WEEKS;
      case "@monthly":
      case "monthly":
      case "months":
        return MONTHS;
      case "@annually":
      case "annually":
      case "@yearly":
      case "yearly":
      case "years":
        return YEARS;

      default:
        return create(json);
    }
  }

  private static Partitioning create(String cronExpression) {
    final CronDefinition cronDefinition = instanceDefinitionFor(CronType.UNIX);
    final Cron cron = new CronParser(cronDefinition).parse(cronExpression);

    return new AutoValue_Partitioning(cronExpression, cron);
  }

  // todo: remove
  public static Set<Partitioning> values() {
    return ImmutableSet.of(HOURS, DAYS, WEEKS, MONTHS);
  }
}
