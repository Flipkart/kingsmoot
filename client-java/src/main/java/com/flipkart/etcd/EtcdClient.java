package com.flipkart.etcd;

import com.ning.http.client.*;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class EtcdClient implements Runnable {

    private static final String LEADER_API = "/v2/members/leader";
    private static final String GET = "/v2/keys/";
    private static final int TIME_OUT = (int) TimeUnit.SECONDS.toMillis(1);
    private final AsyncHttpClient asyncHttpClient;
    private volatile String leader;
    private static long POLLING_INTERVAL = TimeUnit.SECONDS.toMillis(10);
    private String[] uris;
    private volatile boolean closed;
    private Map<String, EtcdWatcher> watchers = new ConcurrentHashMap<>();


    public EtcdClient(String[] uris) throws EtcdException {
        this.uris = uris;
        AsyncHttpClientConfig.Builder builder = new AsyncHttpClientConfig.Builder();
        builder.setConnectionTimeoutInMs(TIME_OUT);
        builder.setRequestTimeoutInMs(TIME_OUT);
        asyncHttpClient = new AsyncHttpClient(builder.build());
        leader = getLeader();
        Thread thread = new Thread(this, "EtcdMasterDiscoveryThread");
        thread.setDaemon(true);
        thread.start();
    }

    public String getLeader() throws EtcdException {
        for (String uri : uris) {
            String url = uri + LEADER_API;
            try {
                Response response = httpGet(url);
                if (response.getStatusCode() == 200) {
                    EtcdLeaderResponse resp = JsonUtil.deserializeJson(response.getResponseBody(), EtcdLeaderResponse.class);
                    return resp.getLeaderUrl();
                }
            } catch (IOException | EtcdException e) {
                //DO nothing
            }
        }
        throw new EtcdException(EtcdErrorCode.ClientInternal, "No leader found");
    }

    public String get(String key) throws EtcdException {
        checkIfLeaderExists();
        String url = leader + GET + key;
        Response response = httpGet(url);
        try {
            if (response.getStatusCode() != 200) {
                if (response.getStatusCode() == 404) {
                    EtcdErrorResponse errorResponse = JsonUtil.deserializeJson(response.getResponseBody(), EtcdErrorResponse.class);
                    throw new EtcdException(errorResponse.getErrorCode(), errorResponse.getMessage());
                } else {
                    throw new EtcdException(EtcdErrorCode.ClientInternal, "Non 2XX response for " + url + " " + response.getStatusCode() + ":" + response.getResponseBody());
                }
            }

            EtcdGetKeyResponse resp = JsonUtil.deserializeJson(response.getResponseBody(), EtcdGetKeyResponse.class);
            return resp.getNode().getValue();
        } catch (IOException e) {
            throw new EtcdException(EtcdErrorCode.ClientInternal, "Error while getting key from etcd", e);
        }
    }

    public synchronized String watch(String key, EtcdKeyChangeListener listener) {
        EtcdWatcher etcdWatcher = watchers.get(key);
        if (null == etcdWatcher) {
            etcdWatcher = new EtcdWatcher(this, key, listener);
            etcdWatcher.start();
            watchers.put(key, etcdWatcher);
        } else {
            etcdWatcher.addListener(listener);
        }
        return etcdWatcher.getCurrentValue();
    }

    private void checkIfLeaderExists() throws EtcdException {
        if (null == leader) {
            throw new EtcdException(EtcdErrorCode.ClientInternal, "No Master found");
        }
    }

    private Response httpGet(String url) throws EtcdException {
        RequestBuilder builder = new RequestBuilder("GET");
        Request geoRequest = builder.setUrl(url).build();
        try {
            return asyncHttpClient.executeRequest(geoRequest).get();
        } catch (IOException | ExecutionException | InterruptedException e) {
            throw new EtcdException(EtcdErrorCode.ClientInternal, "Error while sending request to " + url, e);
        }
    }

    @Override
    public void run() {
        while (!closed) {
            sleep();
            try {
                leader = getLeader();
            } catch (EtcdException e) {
                leader = null;
            }

        }
    }

    private void sleep() {
        try {
            Thread.sleep(POLLING_INTERVAL);
        } catch (InterruptedException e) {
            //Do nothing
        }
    }
}
