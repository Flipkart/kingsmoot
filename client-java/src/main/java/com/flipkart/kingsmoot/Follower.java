package com.flipkart.kingsmoot;

public interface Follower {
    void onLeaderElect(String newLeader);
    void onLeaderDeath();
}
