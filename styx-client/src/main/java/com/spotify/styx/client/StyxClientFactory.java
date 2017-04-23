package com.spotify.styx.client;

import com.spotify.apollo.Client;

/**
 * Created by fdema on 2017-04-21.
 */
public class StyxClientFactory {

  public static StyxClient create(Client client, String apiHost) {
    return new StyxApolloClient(client, apiHost, "");
  }

  public static StyxStatusClient createStatusClient(Client client, String apiHost) {
    return new StyxApolloClient(client, apiHost, "");
  }

  public static StyxBackfillClient createBackfillClient(Client client, String apiHost) {
    return new StyxApolloClient(client, apiHost, "");
  }

  public static StyxSchedulerClient createSchedulerClient(Client client, String apiHost) {
    return new StyxApolloClient(client, apiHost, "");
  }

  public static StyxResourceClient createResourceClient(Client client, String apiHost) {
    return new StyxApolloClient(client, apiHost, "");
  }

  public static StyxWorkflowClient createWorkflowClient(Client client, String apiHost) {
    return new StyxApolloClient(client, apiHost, "");
  }
}
