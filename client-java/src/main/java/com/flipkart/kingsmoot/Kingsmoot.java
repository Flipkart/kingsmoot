package com.flipkart.kingsmoot;


import com.flipkart.etcd.EtcdClient;
import com.flipkart.etcd.EtcdException;

import java.io.Closeable;
import java.io.IOException;

public class Kingsmoot implements Closeable {

    private final EtcdClient etcdClient;

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


    @Override
    public void close() throws IOException {
        etcdClient.close();
    }
}
