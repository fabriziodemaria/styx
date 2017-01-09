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

import static com.spotify.styx.workflow.ParameterUtil.decrementInstant;
import static com.spotify.styx.workflow.ParameterUtil.lastInstant;
import static com.spotify.styx.workflow.ParameterUtil.nextInstant;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.spotify.styx.model.Partitioning;
import java.time.Instant;
import org.junit.Test;

public class ParameterUtilTest {

  private static final Instant TIME = Instant.parse("2016-01-19T09:11:22.333Z");

  @Test
  public void shouldFormatDate() throws Exception {
    final String date = ParameterUtil.formatDate(TIME);

    assertThat(date, is("2016-01-19"));
  }

  @Test
  public void shouldFormatDateTime() throws Exception {
    final String dateTime = ParameterUtil.formatDateTime(TIME);

    assertThat(dateTime, is("2016-01-19T09:11:22.333Z"));
  }

  @Test
  public void shouldFormatDateHour() throws Exception {
    final String dateHour = ParameterUtil.formatDateHour(TIME);

    assertThat(dateHour, is("2016-01-19T09"));
  }

  @Test
  public void shouldDecrementInstant() throws Exception {
    final Instant time = Instant.parse("2016-01-19T08:11:22.333Z");
    final Instant timeMinusDay = Instant.parse("2016-01-18T09:11:22.333Z");
    final Instant timeMinusWeek = Instant.parse("2016-01-12T09:11:22.333Z");
    final Instant timeMinusMonth = Instant.parse("2015-12-19T09:11:22.333Z");

    final Instant hour = decrementInstant(TIME, Partitioning.HOURS);
    assertThat(hour, is(time));

    final Instant day = decrementInstant(TIME, Partitioning.DAYS);
    assertThat(day, is(timeMinusDay));

    final Instant week = decrementInstant(TIME, Partitioning.WEEKS);
    assertThat(week, is(timeMinusWeek));

    final Instant months = decrementInstant(TIME, Partitioning.MONTHS);
    assertThat(months, is(timeMinusMonth));
  }

  @Test
  public void shouldGetLastInstant() throws Exception {
    final Instant lastTimeHours = Instant.parse("2016-01-19T09:00:00.00Z");
    final Instant lastTimeDays = Instant.parse("2016-01-19T00:00:00.00Z");
    final Instant lastTimeWeeks = Instant.parse("2016-01-18T00:00:00.00Z");
    final Instant lastTimeMonths = Instant.parse("2016-01-01T00:00:00.00Z");

    final Instant hour = lastInstant(TIME, Partitioning.HOURS);
    assertThat(hour, is(lastTimeHours));

    final Instant day = lastInstant(TIME, Partitioning.DAYS);
    assertThat(day, is(lastTimeDays));

    final Instant weeks = lastInstant(TIME, Partitioning.WEEKS);
    assertThat(weeks, is(lastTimeWeeks));

    final Instant months = lastInstant(TIME, Partitioning.MONTHS);
    assertThat(months, is(lastTimeMonths));
  }

  @Test
  public void shouldGetNextInstant() throws Exception {
    final Instant nextTimeHours = Instant.parse("2016-01-19T10:00:00.00Z");
    final Instant nextTimeDays = Instant.parse("2016-01-20T00:00:00.00Z");
    final Instant nextTimeWeeks = Instant.parse("2016-01-25T00:00:00.00Z");
    final Instant nextTimeMonths = Instant.parse("2016-02-01T00:00:00.00Z");

    final Instant hour = nextInstant(TIME, Partitioning.HOURS);
    assertThat(hour, is(nextTimeHours));

    final Instant day = nextInstant(TIME, Partitioning.DAYS);
    assertThat(day, is(nextTimeDays));

    final Instant weeks = nextInstant(TIME, Partitioning.WEEKS);
    assertThat(weeks, is(nextTimeWeeks));

    final Instant months = nextInstant(TIME, Partitioning.MONTHS);
    assertThat(months, is(nextTimeMonths));
  }
}
