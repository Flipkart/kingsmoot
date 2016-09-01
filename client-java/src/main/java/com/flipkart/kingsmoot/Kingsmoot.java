package com.flipkart.kingsmoot;


import com.flipkart.etcd.EtcdClient;
import com.flipkart.etcd.EtcdException;

import java.util.concurrent.TimeUnit;

public class Kingsmoot {

    private final EtcdClient etcdClient;
    private static int READ_TIMEOUT = (int) TimeUnit.SECONDS.toMillis(1);

    public Kingsmoot(Conf conf) throws KingsmootException {
        String[] servers = conf.getServers();
        try {
            etcdClient = new EtcdClient(servers);
        } catch (EtcdException e) {
            throw new KingsmootException("Problem while connecting to discovery servers", e);
        }
    }

    public void join(String serviceName, final Follower follower) throws KingsmootException {
        String currentValue = etcdClient.watch(serviceName, new EtcdKeyChangeListenerImpl(follower));
        if (null == currentValue) {
            throw new KingsmootException("No Leader found for " + serviceName);
        }
        follower.onLeaderElect(currentValue);
    }
}
