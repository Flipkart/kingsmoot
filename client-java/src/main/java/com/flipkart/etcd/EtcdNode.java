package com.flipkart.etcd;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class EtcdNode {
    private String key;
    private String value;
    private long modifiedIndex;
    private long createdIndex;
}
