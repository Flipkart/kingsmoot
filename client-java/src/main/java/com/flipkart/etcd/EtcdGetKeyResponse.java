package com.flipkart.etcd;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class EtcdGetKeyResponse {
    private EtcdKeyAction action;
    private EtcdNode node;

}
