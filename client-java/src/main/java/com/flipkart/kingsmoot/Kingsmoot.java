package com.flipkart.kingsmoot;

import mousio.client.retry.RetryWithTimeout;
import mousio.etcd4j.EtcdClient;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class Kingsmoot {

    private Map<String, EtcdWatcher> followers = new ConcurrentHashMap<>();
    private final EtcdClient etcdClient;
    private static int READ_TIMEOUT = (int) TimeUnit.SECONDS.toMillis(1);

    public Kingsmoot(Conf conf) {
        String[] servers = conf.getServers();
        List<URI> serverUris = new ArrayList<>();
        for (String server : servers) {
            serverUris.add(URI.create(server));
        }
        etcdClient = new EtcdClient(serverUris.toArray(new URI[serverUris.size()]));
        etcdClient.setRetryHandler(new RetryWithTimeout(READ_TIMEOUT, READ_TIMEOUT));
    }

    public void join(String serviceName, final Follower follower) throws KingsmootException {
        if (followers.containsKey(serviceName)) {
            throw new KingsmootException("A follower has already joined for the service " + serviceName);
        }
        EtcdWatcher watcher = new EtcdWatcher(etcdClient, serviceName, follower);
        watcher.start();
        followers.put(serviceName, watcher);
    }
}
