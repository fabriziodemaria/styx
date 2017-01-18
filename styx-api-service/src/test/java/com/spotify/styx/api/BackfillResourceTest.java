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

import static com.github.npathai.hamcrestopt.OptionalMatchers.hasValue;
import static com.spotify.apollo.test.unit.ResponseMatchers.hasStatus;
import static com.spotify.apollo.test.unit.StatusTypeMatchers.belongsToFamily;
import static com.spotify.styx.api.JsonMatchers.assertJson;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyQuery;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.testing.LocalDatastoreHelper;
import com.spotify.apollo.Environment;
import com.spotify.apollo.Response;
import com.spotify.apollo.StatusType;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.storage.AggregateStorage;
import com.spotify.styx.util.Json;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import okio.ByteString;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class BackfillResourceTest extends VersionedApiTest {

  private static LocalDatastoreHelper localDatastore;

  private AggregateStorage storage = new AggregateStorage(
      mock(Connection.class),
      localDatastore.options().service(),
      Duration.ZERO);

  private static final Backfill BACKFILL_1 = Backfill.create("backfill1",
                                                             Instant.parse("2017-01-01T00:00:00Z"),
                                                             Instant.parse("2017-01-02T00:00:00Z"),
                                                             "component", "workflow1", 1,
                                                             Optional.empty(),
                                                             false, "backfill1");
  private static final Backfill BACKFILL_2 = Backfill.create("backfill2",
                                                             Instant.parse("2017-01-01T00:00:00Z"),
                                                             Instant.parse("2017-02-01T00:00:00Z"),
                                                             "component", "workflow2", 1,
                                                             Optional.empty(),
                                                             false, "backfill2");

  public BackfillResourceTest(Api.Version version) {
    super(BackfillResource.BASE, version, "backfill-test");
  }

  @Override
  void init(Environment environment) {
    environment.routingEngine().registerRoutes(new BackfillResource(storage).routes());
  }

  @BeforeClass
  public static void setUpClass() throws Exception {
    localDatastore = LocalDatastoreHelper.create(1.0); // 100% global consistency
    localDatastore.start();
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    if (localDatastore != null) {
      localDatastore.stop();
    }
  }

  @Before
  public void setUp() throws Exception {
    storage.storeBackfill(BACKFILL_1);
  }

  @After
  public void tearDown() throws Exception {
    // clear datastore after each test
    Datastore datastore = localDatastore.options().service();
    KeyQuery query = Query.keyQueryBuilder().build();
    final QueryResults<Key> keys = datastore.run(query);
    while (keys.hasNext()) {
      datastore.delete(keys.next());
    }
  }

  @Test
  public void shouldListBackfills() throws Exception {
    sinceVersion(Api.Version.V1);

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("")));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(response, "backfills[0].id", equalTo("backfill1"));
  }

  @Test
  public void shouldListMultipleBackfills() throws Exception {
    sinceVersion(Api.Version.V1);

    storage.storeBackfill(BACKFILL_2);
    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("")));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(response, "backfills", hasSize(2));
  }

  @Test
  public void shouldGetBackfill() throws Exception {
    sinceVersion(Api.Version.V1);

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("/backfill1")));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(response, "id", equalTo("backfill1"));
  }

  @Test
  public void shouldUseGeneratedValuesWhenPosting() throws Exception {
    sinceVersion(Api.Version.V1);

    Backfill backfill = Backfill.create(null, BACKFILL_1.start(), BACKFILL_1.end(), "component", "workflow", 10, Optional.empty(), null, null);
    String jsonWithMissingValues = Json.OBJECT_MAPPER.writeValueAsString(backfill);

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("POST", path(""),
                                            ByteString.encodeUtf8(jsonWithMissingValues)));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    Backfill postedBackfill = Json.OBJECT_MAPPER.readValue(
        response.payload().get().toByteArray(), Backfill.class);

    assertTrue(postedBackfill.id().matches("backfill-[\\d-]+"));
    assertFalse(postedBackfill.dockerImage().isPresent());
  }

  @Test
  public void shouldPostBackfill() throws Exception {
    sinceVersion(Api.Version.V1);

    String json = Json.OBJECT_MAPPER.writeValueAsString(BACKFILL_2);

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("POST", path(""),
                                            ByteString.encodeUtf8(json)));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertThat(storage.backfill("backfill2"), hasValue(BACKFILL_2));
  }

  @Test
  public void shouldUpdateBackfill() throws Exception {
    sinceVersion(Api.Version.V1);

    assertThat(storage.backfill(BACKFILL_1.id()).get().processed(), equalTo(false));

    String json =
        "{\"id\":\"backfill1\",\"start\":\"2017-01-01T00:00:00Z\",\"end\":\"2017-01-02T00:00:00Z\",\"component\":\"component\",\"workflow\":\"workflow1\",\"concurrency\":4,\"docker_image\":null,\"processed\":true,\"resource\":\"backfill1\"}";

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("PUT", path("/backfill1"),
                                            ByteString.encodeUtf8(json)));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(response, "id", equalTo("backfill1"));
    assertJson(response, "concurrency", equalTo(4));
    assertJson(response, "processed", equalTo(true));

    assertThat(storage.backfill(BACKFILL_1.id()).get().processed(), equalTo(true));
  }
}
