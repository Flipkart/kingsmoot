package com.flipkart.etcd;

import lombok.Data;

@Data
public class EtcdKeyChangedEvent {
    private final EtcdKeyAction action;
    private final String prevValue;
    private final String newValue;
}
