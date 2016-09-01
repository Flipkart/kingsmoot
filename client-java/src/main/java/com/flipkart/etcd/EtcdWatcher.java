package com.flipkart.etcd;

import com.flipkart.kingsmoot.KingsmootException;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class EtcdWatcher implements Runnable {
    private static long POLLING_INTERVAL = TimeUnit.SECONDS.toMillis(10);
    private EtcdClient client;
    private final String key;
    private final List<EtcdKeyChangeListener> listeners = new ArrayList<>();
    @Getter
    private String currentValue;
    private boolean closed;

    public EtcdWatcher(EtcdClient client, String key, EtcdKeyChangeListener listener) {
        this.client = client;
        this.key = key;
        listeners.add(listener);
    }

    public synchronized void addListener(EtcdKeyChangeListener listener) {
        listeners.add(listener);
    }

    public String start() {
        currentValue = getValue(this.key);
        Thread etcdWatcher = new Thread(this, "EtcdWatcher-" + key);
        etcdWatcher.setDaemon(true);
        etcdWatcher.start();
        return currentValue;
    }


    @Override
    public void run() {
        while (!closed) {
            try {
                String newValue = getValue(this.key);
                notifyIfRequired(newValue);
            } catch (KingsmootException e) {
                notifyIfRequired(null);
            } catch (Throwable e) {
                //Do nothing
            }
            sleep();
        }
    }

    private void sleep() {
        try {
            Thread.sleep(POLLING_INTERVAL);
        } catch (InterruptedException e) {
            //Do nothing
        }
    }

    private String getValue(String key) {
        try {
            return client.get(key);
        } catch (EtcdException e) {
            if (e.getErrorCode() == EtcdErrorCode.KeyNotFound) {
                return null;
            } else {
                throw new KingsmootException("Error while getting value for " + this.key, e);
            }
        }
    }


    private synchronized void notifyIfRequired(String newValue) {
        if (null == currentValue) {
            if (null != newValue) {
                EtcdKeyChangedEvent created = new EtcdKeyChangedEvent(EtcdKeyAction.create, currentValue, newValue);
                for (EtcdKeyChangeListener listener : listeners) {
                    listener.onChange(created);
                }
            }
        } else {
            if (null == newValue) {
                EtcdKeyChangedEvent deleted = new EtcdKeyChangedEvent(EtcdKeyAction.delete, currentValue, newValue);
                for (EtcdKeyChangeListener listener : listeners) {
                    listener.onChange(deleted);
                }
            } else if (!newValue.equals(this.currentValue)) {
                EtcdKeyChangedEvent updated = new EtcdKeyChangedEvent(EtcdKeyAction.update, currentValue, newValue);
                for (EtcdKeyChangeListener listener : listeners) {
                    listener.onChange(updated);
                }
            }
        }
        this.currentValue = newValue;
    }
}
