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
import static com.github.npathai.hamcrestopt.OptionalMatchers.isEmpty;
import static com.spotify.apollo.test.unit.ResponseMatchers.hasStatus;
import static com.spotify.apollo.test.unit.StatusTypeMatchers.belongsToFamily;
import static com.spotify.styx.api.JsonMatchers.assertJson;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyQuery;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.testing.LocalDatastoreHelper;
import com.google.common.collect.ImmutableMap;
import com.spotify.apollo.Environment;
import com.spotify.apollo.Response;
import com.spotify.apollo.StatusType;
import com.spotify.styx.model.Resource;
import com.spotify.styx.storage.AggregateStorage;
import java.time.Duration;
import okio.ByteString;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ResourceResourceTest extends VersionedApiTest {

  private static LocalDatastoreHelper localDatastore;

  private AggregateStorage storage = new AggregateStorage(
      mock(Connection.class),
      localDatastore.getOptions().getService(),
      Duration.ZERO);

  private static final Resource RESOURCE_1 = Resource.create("resource1", 1);
  private static final Resource RESOURCE_2 = Resource.create("resource2", 2);

  public ResourceResourceTest(Api.Version version) {
    super(ResourceResource.BASE, version, "resource-test");
  }

  @Override
  protected void init(Environment environment) {
    ResourceResource resourceResource = new ResourceResource(storage);

    environment.routingEngine()
        .registerRoutes(Api.withCommonMiddleware(
            resourceResource.routes()));
  }

  @BeforeClass
  public static void setUpClass() throws Exception {
    localDatastore = LocalDatastoreHelper.create(1.0); // 100% global consistency
    localDatastore.start();
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    if (localDatastore != null) {
      try {
        localDatastore.stop(org.threeten.bp.Duration.ofSeconds(30));
      } catch (Throwable e) {
        e.printStackTrace();
      }
    }
  }

  @Before
  public void setUp() throws Exception {
    storage.storeResource(RESOURCE_1);
  }

  @After
  public void tearDown() throws Exception {
    // clear datastore after each test
    Datastore datastore = localDatastore.getOptions().getService();
    KeyQuery query = Query.newKeyQueryBuilder().build();
    final QueryResults<Key> keys = datastore.run(query);
    while (keys.hasNext()) {
      datastore.delete(keys.next());
    }
  }

  @Test
  public void shouldListResources() throws Exception {
    sinceVersion(Api.Version.V3);

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("")));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(response, "resources[0].id", equalTo("resource1"));
    assertJson(response, "resources[0].concurrency", equalTo(1));
  }

  @Test
  public void shouldListMultipleResources() throws Exception {
    sinceVersion(Api.Version.V3);

    storage.storeResource(RESOURCE_2);
    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("")));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(response, "resources", hasSize(2));
    assertJson(response, "resources", containsInAnyOrder(
        ImmutableMap.of("id", "resource1", "concurrency", 1),
        ImmutableMap.of("id", "resource2", "concurrency", 2)
    ));
  }

  @Test
  public void shouldGetResource() throws Exception {
    sinceVersion(Api.Version.V3);

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("/resource1")));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(response, "id", equalTo("resource1"));
    assertJson(response, "concurrency", equalTo(1));
  }

  @Test
  public void shouldPostResource() throws Exception {
    sinceVersion(Api.Version.V3);

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("POST", path(""),
            ByteString.encodeUtf8("{\"id\": \"resource2\", \"concurrency\": 2}")));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(response, "id", equalTo("resource2"));
    assertJson(response, "concurrency", equalTo(2));

    assertThat(storage.resource("resource2"), hasValue(RESOURCE_2));
  }

  @Test
  public void shouldUpdateResource() throws Exception {
    sinceVersion(Api.Version.V3);

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("PUT", path("/resource1"),
            ByteString.encodeUtf8("{\"id\": \"resource1\", \"concurrency\": 21}")));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(response, "id", equalTo("resource1"));
    assertJson(response, "concurrency", equalTo(21));

    assertThat(storage.resource(RESOURCE_1.id()), hasValue(Resource.create(RESOURCE_1.id(), 21)));
  }

  @Test
  public void shouldDeleteResource() throws Exception {
    sinceVersion(Api.Version.V3);

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("DELETE", path("/resource1")));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));

    assertThat(storage.resource(RESOURCE_1.id()).isPresent(), is(false));
  }

  @Test
  public void shouldHandleMultipleResourcesIndependently() throws Exception {
    sinceVersion(Api.Version.V3);

    // add resource2
    awaitResponse(serviceHelper.request("POST", path(""),
        ByteString.encodeUtf8("{\"id\": \"resource2\", \"concurrency\": 2}")));

    // make sure both are listed
    Response<ByteString> listResponse =
        awaitResponse(serviceHelper.request("GET", path("")));
    assertThat(listResponse, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(listResponse, "resources", hasSize(2));
    assertJson(listResponse, "resources", containsInAnyOrder(
        ImmutableMap.of("id", "resource1", "concurrency", 1),
        ImmutableMap.of("id", "resource2", "concurrency", 2)
    ));

    // change resource2
    Response<ByteString> putResponse =
        awaitResponse(serviceHelper.request("PUT", path("/resource2"),
            ByteString.encodeUtf8("{\"id\": \"resource2\", \"concurrency\": 3}")));
    assertThat(putResponse, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));

    // make sure resource2 is changed in list and that resource1 is unchanged
    Response<ByteString> listResponse2 =
        awaitResponse(serviceHelper.request("GET", path("")));
    assertThat(listResponse2, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(listResponse2, "resources", hasSize(2));
    assertJson(listResponse2, "resources", containsInAnyOrder(
        ImmutableMap.of("id", "resource1", "concurrency", 1),
        ImmutableMap.of("id", "resource2", "concurrency", 3)
    ));
    assertThat(storage.resource(RESOURCE_1.id()), hasValue(RESOURCE_1));
    assertThat(storage.resource("resource2"), hasValue(Resource.create("resource2", 3)));

    // delete resource2
    Response<ByteString> deleteResponse =
        awaitResponse(serviceHelper.request("DELETE", path("/resource2")));
    assertThat(deleteResponse, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));

    // make sure resource2 is not showing up in the listing
    Response<ByteString> listResponse3 =
        awaitResponse(serviceHelper.request("GET", path("")));
    assertThat(listResponse3, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(listResponse3, "resources", hasSize(1));
    assertJson(listResponse3, "resources", containsInAnyOrder(
        ImmutableMap.of("id", "resource1", "concurrency", 1)
    ));
    assertThat(storage.resource(RESOURCE_1.id()), hasValue(RESOURCE_1));
    assertThat(storage.resource("resource2"), isEmpty());
  }
}
