package com.flipkart.kingsmoot;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class KingsmootTest {


    public static final String AKEM = "akem";

    static class MyFollower implements Follower {
        private ArrayBlockingQueue<String> notifications = new ArrayBlockingQueue<String>(10);

        @Override
        public void onLeaderElect(String newLeader) {
            System.out.println("MyFollower.onLeaderElect-->" + newLeader);
            notifications.add(newLeader);
        }

        @Override
        public void onLeaderDeath() {
            System.out.println("MyFollower.onLeaderDeath");
            notifications.add("NULL");
        }

        public String getNotification() throws InterruptedException {
            return notifications.poll(20, TimeUnit.SECONDS);
        }

    }

    DefaultExecutor executor = new DefaultExecutor();

    @BeforeMethod
    public void setUp() throws Exception {
        startEtcd();
    }

    private void startEtcd() throws IOException, InterruptedException {
        CommandLine pwd = CommandLine.parse("../src/kingsmoot/etcd.sh start");
        executor.execute(pwd);
        Thread.sleep(2000);
    }

    @AfterMethod
    public void tearDown() throws Exception {
        stopEtcd();
    }

    private void stopEtcd() throws IOException {
        CommandLine pwd = CommandLine.parse("../src/kingsmoot/etcd.sh stop");
        executor.execute(pwd);
    }

    @Test
    public void testEtcdWatcher() throws Exception {
        String leader1 = "akem1";
        etcdSet(AKEM, leader1);
        MyFollower myFollower = new MyFollower();
        Kingsmoot kingsmoot = new Kingsmoot(new Conf(new String[]{"http://localhost:2369"}, Backend.ETCD));
        kingsmoot.join(AKEM, myFollower);
        Assert.assertEquals(myFollower.getNotification(), leader1);
        stopEtcd();
        Assert.assertEquals(myFollower.getNotification(), "NULL");
        startEtcd();
        etcdSet(AKEM, leader1);
        Assert.assertEquals(myFollower.getNotification(), leader1);
        String leader2 = "akem2";
        etcdSet(AKEM, leader2);
        Assert.assertEquals(myFollower.getNotification(), leader2);
        etcdSet(AKEM, leader2);
        Assert.assertEquals(myFollower.getNotification(), null);
    }

    private void etcdSet(String key, String value) throws IOException {
        CommandLine etcdset = CommandLine.parse("etcdctl --endpoints \"http://127.0.0.1:2369\" set " + key + " " + value);
        executor.execute(etcdset);
    }

}
