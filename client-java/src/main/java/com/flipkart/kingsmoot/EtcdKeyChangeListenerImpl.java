package com.flipkart.kingsmoot;

import com.flipkart.etcd.EtcdKeyChangeListener;
import com.flipkart.etcd.EtcdKeyChangedEvent;

public class EtcdKeyChangeListenerImpl implements EtcdKeyChangeListener {
    private Follower follower;

    public EtcdKeyChangeListenerImpl(Follower follower) {
        this.follower = follower;
    }

    @Override
    public void onChange(EtcdKeyChangedEvent event) {
        switch (event.getAction()) {
            case create:
            case update:
                follower.onLeaderElect(event.getNewValue());
                break;
            case delete:
                follower.onLeaderDeath();
                break;
        }
    }
}
