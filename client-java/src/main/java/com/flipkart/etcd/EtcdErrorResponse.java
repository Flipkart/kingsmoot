package com.flipkart.etcd;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class EtcdErrorResponse {
    private int errorCode;
    private String message;
    private String cause;
    private long index;
}
