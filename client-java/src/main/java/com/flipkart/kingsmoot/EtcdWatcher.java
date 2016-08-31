package com.flipkart.kingsmoot;

import mousio.client.promises.ResponsePromise;
import mousio.etcd4j.EtcdClient;
import mousio.etcd4j.promises.EtcdResponsePromise;
import mousio.etcd4j.requests.EtcdKeyGetRequest;
import mousio.etcd4j.responses.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class EtcdWatcher implements Runnable, ResponsePromise.IsSimplePromiseResponseHandler<EtcdKeysResponse> {
    private static long POLLING_INTERVAL = TimeUnit.SECONDS.toMillis(10);
    private EtcdClient client;
    private final String key;
    private final Follower follower;
    private String currentValue;

    public EtcdWatcher(EtcdClient client, String key, Follower follower) {
        this.client = client;
        this.key = key;
        this.follower = follower;
    }

    public void start() {
        String newValue = getValue(this.key);
        notifyIfRequired(newValue);
        waitForChange(this.key);
        new Thread(this, "EtcdWatcher").start();
    }

    private void waitForChange(String key) {
        boolean watchSet = false;
        while (!watchSet) {
            EtcdKeyGetRequest etcdKeyGetRequest = client.get(key);
            etcdKeyGetRequest.waitForChange();
            try {
                EtcdResponsePromise<EtcdKeysResponse> responsePromise = etcdKeyGetRequest.send();
                responsePromise.addListener(this);
                watchSet = true;
            } catch (IOException e) {
                sleep();
            }
        }
    }


    @Override
    public void run() {
        while (true) {
            try {
                String newValue = getValue(this.key);
                notifyIfRequired(newValue);
            } catch (Throwable e) {
                //Do nothing
            }
            sleep();
        }
    }

    private void sleep() {
        try {
            Thread.sleep(POLLING_INTERVAL);
        } catch (InterruptedException e) {
            //Do nothing
        }
    }

    private String getValue(String key) {
        EtcdKeyGetRequest etcdKeyGetRequest = client.get(key);
        try {
            EtcdKeysResponse etcdKeysResponse = etcdKeyGetRequest.send().get();
            return etcdKeysResponse.getNode().getValue();
        } catch (EtcdException e) {
            if (e.errorCode == EtcdErrorCode.KeyNotFound) {
                return null;
            } else {
                throw new KingsmootException("Error while getting value for " + this.key, e);
            }
        } catch (IOException | EtcdAuthenticationException | TimeoutException e) {
            throw new KingsmootException("Error while getting value for" + this.key, e);
        }
    }

    @Override
    public void onResponse(ResponsePromise<EtcdKeysResponse> response) {
        EtcdKeysResponse responseNow = response.getNow();
        if (null == responseNow) {
            notifyIfRequired(null);
        } else {
            EtcdKeyAction action = responseNow.getAction();
            switch (action) {
                case set:
                case create:
                case update:
                case compareAndSwap:
                    EtcdKeysResponse.EtcdNode node = responseNow.getNode();
                    String newValue = node.getValue();
                    notifyIfRequired(newValue);
                    break;
                case delete:
                case expire:
                case compareAndDelete:
                    notifyIfRequired(null);
                    break;
            }
        }
        waitForChange(key);
    }

    private synchronized void notifyIfRequired(String newValue) {
        if (null == newValue) {
            if (null != this.currentValue) {
                this.follower.onLeaderDeath();

            }
        } else if (!newValue.equals(this.currentValue)) {
            this.follower.onLeaderElect(newValue);
        }
        this.currentValue = newValue;
    }
}
