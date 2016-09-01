package com.flipkart.etcd;

public interface EtcdKeyChangeListener {
    void onChange(EtcdKeyChangedEvent event);
}
