package com.flipkart.etcd;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
public class EtcdLeaderResponse {
    private String name;
    private String id;
    private List<String> peerURLs;
    private List<String> clientURLs;

    public String getLeaderUrl() {
        return clientURLs.iterator().next();
    }
}
