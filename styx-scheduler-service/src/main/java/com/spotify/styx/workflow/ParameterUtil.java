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

package com.spotify.styx.workflow;

import static com.spotify.styx.model.Partitioning.DAYS;
import static com.spotify.styx.model.Partitioning.HOURS;
import static com.spotify.styx.model.Partitioning.MONTHS;
import static com.spotify.styx.model.Partitioning.WEEKS;
import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.YEAR;

import com.cronutils.model.time.ExecutionTime;
import com.spotify.styx.model.Partitioning;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowInstance;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import javaslang.control.Either;

/**
 * Helpers for working with {@link com.spotify.styx.model.Workflow} parameter handling.
 */
public final class ParameterUtil {

  public static final String HOUR_PATTERN = "yyyy-MM-dd'T'HH";
  public static final String DAY_PATTERN = "yyyy-MM-dd";

  private ParameterUtil() {
  }

  private static final int MIN_YEAR_WIDTH = 4;
  private static final int MAX_YEAR_WIDTH = 10;
  private static final DateTimeFormatter ISO_LOCAL_MONTH = new DateTimeFormatterBuilder()
      .appendValue(YEAR, MIN_YEAR_WIDTH, MAX_YEAR_WIDTH, SignStyle.EXCEEDS_PAD)
      .appendLiteral('-')
      .appendValue(MONTH_OF_YEAR, 2)
      .toFormatter();

  private static final DateTimeFormatter ISO_LOCAL_DATE_HOUR = new DateTimeFormatterBuilder()
      .append(DateTimeFormatter.ISO_LOCAL_DATE)
      .appendLiteral('T')
      .appendValue(ChronoField.HOUR_OF_DAY, 2)
      .toFormatter();

  public static String formatDateTime(Instant instant) {
    return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(
        instant.atOffset(UTC));
  }

  public static String formatDate(Instant instant) {
    return DateTimeFormatter.ISO_LOCAL_DATE.format(
        instant.atOffset(UTC));
  }

  public static String formatDateHour(Instant instant) {
    return ISO_LOCAL_DATE_HOUR.format(
        instant.truncatedTo(ChronoUnit.HOURS)
            .atOffset(UTC));
  }

  public static String formatMonth(Instant instant) {
    return ISO_LOCAL_MONTH.format(
        instant.truncatedTo(ChronoUnit.DAYS)
            .atOffset(UTC));
  }

  /**
   * Given a {@link Workflow} with certain frequency / {@link Partitioning},
   * it returns an instant that is 1 {@link TemporalUnit} less.
   * e.g. Given an instant '2016-10-10T15:00:000' and hourly {@link Partitioning}, the adjusted
   * instant will return '2016-10-10T14:00:000'.
   *
   * <p>todo: used only for parameter passing (previous partition)
   *
   * @param instant The instant to adjust.
   * @param partitioning The frequency unit to adjust the instant for.
   */
  public static Instant decrementInstant(Instant instant, Partitioning partitioning) {
    LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneId.of(UTC.toString()));
    dateTime = dateTime.minus(1, partitioningToTemporalUnit(partitioning));
    Instant updatedInstant = dateTime.atZone(ZoneId.of(UTC.toString())).toInstant();
    return updatedInstant;
  }

  /**
   * Converts {@link Partitioning} to {@link ChronoUnit}.
   */
  private static TemporalUnit partitioningToTemporalUnit(Partitioning partitioning) {
    if (partitioning.equals(HOURS)) {
      return ChronoUnit.HOURS;
    } else if (partitioning.equals(DAYS)) {
      return ChronoUnit.DAYS;
    } else if (partitioning.equals(WEEKS)) {
      return ChronoUnit.WEEKS;
    } else if (partitioning.equals(MONTHS)) {
      return ChronoUnit.MONTHS;
    } else {
      throw new IllegalArgumentException("Partitioning not supported: " + partitioning);
    }
  }

  /**
   * Gets the last execution instant for a {@link Partitioning}, relative to a given instant.
   *
   * <p>e.g. an hourly partitioning has a last execution instant at 13:00 relative to 13:22.
   *
   * @param instant      The instant to calculate the last execution instant relative to
   * @param partitioning The partitioning of executions
   * @return an instant at the last execution time
   */
  public static Instant lastInstant(Instant instant, Partitioning partitioning) {
    final ExecutionTime executionTime = ExecutionTime.forCron(partitioning.cron());
    final ZonedDateTime utcDateTime = instant.atZone(UTC);
    final ZonedDateTime lastDateTime = executionTime.lastExecution(utcDateTime);

    return lastDateTime.toInstant();
  }

  /**
   * Gets the next execution instant for a {@link Partitioning}, relative to a given instant.
   *
   * <p>e.g. an hourly partitioning has a next execution instant at 14:00 relative to 13:22.
   *
   * @param instant      The instant to calculate the next execution instant relative to
   * @param partitioning The partitioning of executions
   * @return an instant at the next execution time
   */
  public static Instant nextInstant(Instant instant, Partitioning partitioning) {
    final ExecutionTime executionTime = ExecutionTime.forCron(partitioning.cron());
    final ZonedDateTime utcDateTime = instant.atZone(UTC);
    final ZonedDateTime nextDateTime = executionTime.nextExecution(utcDateTime);

    return nextDateTime.toInstant();
  }

  public static Either<String, Instant> instantFromWorkflowInstance(
      WorkflowInstance workflowInstance,
      Partitioning partitioning) {
    if (partitioning.equals(HOURS)) {
      try {
        final LocalDateTime localDateTime = LocalDateTime.parse(
            workflowInstance.parameter(),
            DateTimeFormatter.ofPattern(HOUR_PATTERN));
        return Either.right(localDateTime.toInstant(UTC));
      } catch (DateTimeParseException e) {
        return Either.left(parseErrorMessage(partitioning, HOUR_PATTERN));
      }

    } else if (partitioning.equals(DAYS)) {
      try {
        final LocalDate localDate = LocalDate.parse(
            workflowInstance.parameter(),
            DateTimeFormatter.ofPattern(DAY_PATTERN));
        return Either.right(localDate.atStartOfDay().toInstant(UTC));
      } catch (DateTimeParseException e) {
        return Either.left(parseErrorMessage(partitioning, DAY_PATTERN));
      }

    } else if (partitioning.equals(WEEKS)) {
      try {
        LocalDate localDate = LocalDate.parse(
            workflowInstance.parameter(),
            DateTimeFormatter.ofPattern(DAY_PATTERN));
        int daysToSubtract = localDate.getDayOfWeek().getValue();
        localDate = localDate.minusDays(daysToSubtract - 1);
        return Either.right(localDate.atStartOfDay().toInstant(UTC));
      } catch (DateTimeParseException e) {
        return Either.left(parseErrorMessage(partitioning, DAY_PATTERN));
      }

    } else {
      return Either.left("Partitioning not supported: " + partitioning);
    }
  }

  private static String parseErrorMessage(Partitioning partitioning, String pattern) {
    return String.format(
        "Cannot parse time parameter. Expected partitioning is %s: %s",
        partitioning,
        pattern);
  }
}
