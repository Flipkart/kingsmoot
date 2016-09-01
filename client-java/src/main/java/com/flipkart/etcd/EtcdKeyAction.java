package com.flipkart.etcd;

public enum EtcdKeyAction {
    set,
    get,
    create,
    update,
    delete,
    expire,
    compareAndSwap,
    compareAndDelete
}