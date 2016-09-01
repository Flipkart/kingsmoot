package com.flipkart.etcd;

import lombok.Getter;

public class EtcdException extends Exception {
    @Getter
    private int errorCode;

    public EtcdException(int errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public EtcdException(int errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }


}
