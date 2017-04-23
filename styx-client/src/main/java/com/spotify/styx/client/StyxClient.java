package com.spotify.styx.client;

/**
 * Created by fdema on 2017-04-21.
 */
public interface StyxClient
    extends StyxStatusClient, StyxSchedulerClient, StyxResourceClient, StyxBackfillClient,
            StyxWorkflowClient {
}
