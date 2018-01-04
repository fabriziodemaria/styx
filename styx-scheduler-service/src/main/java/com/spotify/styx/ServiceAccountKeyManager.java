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

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.iam.v1.Iam;
import com.google.api.services.iam.v1.model.CreateServiceAccountKeyRequest;
import com.google.api.services.iam.v1.model.ServiceAccountKey;
import com.spotify.styx.util.GcpUtil;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceAccountKeyManager {

  private static final Logger LOG = LoggerFactory.getLogger(ServiceAccountKeyManager.class);

  private final Iam iam;

  public ServiceAccountKeyManager(Iam iam) {
    this.iam = iam;
  }

  public ServiceAccountKey createJsonKey(String serviceAccount) throws IOException {
    return createKey(serviceAccount, new CreateServiceAccountKeyRequest()
        .setPrivateKeyType("TYPE_GOOGLE_CREDENTIALS_FILE"));
  }

  public ServiceAccountKey createP12Key(String serviceAccount) throws IOException {
    return createKey(serviceAccount, new CreateServiceAccountKeyRequest()
        .setPrivateKeyType("TYPE_PKCS12_FILE"));
  }

  public boolean serviceAccountExists(String serviceAccount) throws IOException {
    try {
      iam.projects().serviceAccounts().get("projects/-/serviceAccounts/" + serviceAccount)
          .execute();
      return true;
    } catch (GoogleJsonResponseException e) {
      if (e.getStatusCode() == 404) {
        return false;
      }
      throw e;
    }
  }

  public boolean keyExists(String keyName) throws IOException {
    try {
      iam.projects().serviceAccounts().keys()
          .get(keyName)
          .execute();
      return true;
    } catch (GoogleJsonResponseException e) {
      // TODO: handle 403 correctly once google fixes their API
      if (e.getStatusCode() == 403 || e.getStatusCode() == 404) {
        return false;
      }
      throw e;
    }
  }

  private ServiceAccountKey createKey(String serviceAccount,
      CreateServiceAccountKeyRequest request)
      throws IOException {
    return iam.projects().serviceAccounts().keys()
        .create("projects/-/serviceAccounts/" + serviceAccount, request)
        .execute();
  }

  /**
   * Attempt to delete a service account key as a best effort procedure. Exceptions are logged but
   * not re-thrown.
   * @param keyName The fully qualified name of the key to delete.
   */
  public void tryDeleteKey(String keyName) {
    LOG.info("[AUDIT] Deleting service account key: {}", keyName);
    try {
      iam.projects().serviceAccounts().keys()
          .delete(keyName)
          .execute();
    } catch (GoogleJsonResponseException e) {
      // TODO: handle 403 correctly once google fixes their API
      if (e.getStatusCode() == 403 || e.getStatusCode() == 404) {
        LOG.debug("Couldn't find key to delete {}", keyName);
        return;
      }
      LOG.warn("[AUDIT] Failed to delete key {}", keyName, e);
    } catch (Exception e) {
      LOG.warn("[AUDIT] Failed to delete key {}", keyName, e);
    }
  }
}
