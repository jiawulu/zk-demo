package com.example.zk;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.BadVersionException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author wuzhong on 2018/5/11.
 * @version 1.0
 */
public class ZkTest {

    @Test
    public void test() throws IOException, InterruptedException, KeeperException {

        ZooKeeper zooKeeper = getZooKeeper();

        String s = zooKeeper.create("/java", "Hi java".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        Stat exists = zooKeeper.exists("/java", true);
        Stat stat = zooKeeper.setData("/java", "Hi java2".getBytes(), exists.getVersion());

        byte[] data = zooKeeper.getData("/java", false, stat);
        System.out.println(new String(data));

        Assert.assertEquals("Hi java2", new String(data));

        //zooKeeper.delete("/java", 100);
        //new CountDownLatch(1).await();

    }

    @Test(expected = BadVersionException.class)
    public void testUpdate() throws IOException, InterruptedException, KeeperException {

        ZooKeeper zooKeeper = getZooKeeper();
        String s = zooKeeper.create("/java", "Hi java".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        Stat exists = zooKeeper.exists("/java", true);
        Stat stat = zooKeeper.setData("/java", "Hi java2".getBytes(), exists.getVersion() + 1);

    }

    private ZooKeeper getZooKeeper() throws IOException, KeeperException, InterruptedException {
        ZooKeeper zooKeeper = new ZooKeeper("11.239.178.235:4180", 2000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.err.println(event.getPath());
                System.err.println(event.getType());
                System.out.println("on event : " + event.toString());
            }
        });

        Stat java = zooKeeper.exists("/java", true);
        if (null != java) {
            zooKeeper.delete("/java", java.getVersion());
        }
        return zooKeeper;
    }

    //private void syncOperations(ZooKeeper zk, Watcher watcher) throws Exception {
    //
    //    Stat stat = zk.exists("/test", true);
    //    // 创建节点
    //    if (stat == null) {
    //        // CreateMode
    //        // PERSISTENT : 持久
    //        // PERSISTENT_SEQUENTIAL : 持久顺序
    //        // EPHEMERAL : 临时
    //        // EPHEMERAL_SEQUENTIAL : 临时顺序
    //        zk.create("/hifreud", "Hi Freud".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    //    }
    //
    //    Thread.sleep(WAITING * SECOND);
    //    stat = zk.exists("/hifreud", true);
    //    // 获得和修改节点内容
    //    if (stat != null) {
    //        System.out.println("Path 'hifreud' data before change : " + new String(zk.getData("/hifreud", true,
    // stat)));
    //        zk.setData("/hifreud", "Hi Freud Again".getBytes(), stat.getVersion());
    //        System.out.println("Path 'hifreud' data after change : " + new String(zk.getData("/hifreud", true,
    // stat)));
    //    }
    //
    //    Thread.sleep(WAITING * SECOND);
    //    stat = zk.exists("/hifreud", true);
    //    // 创建子节点
    //    if (stat != null) {
    //        if (zk.exists("/hifreud/hi", watcher) == null) {
    //            zk.create("/hifreud/hi", "Hi Freud - hi".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    //        }
    //    }
    //
    //    Thread.sleep(WAITING * SECOND);
    //    stat = zk.exists("/hifreud/hi", true);
    //    // 删除子节点
    //    if (stat != null) {
    //        zk.delete("/hifreud/hi", stat.getVersion());
    //    }
    //
    //    Thread.sleep(WAITING * SECOND);
    //    stat = zk.exists("/hifreud", true);
    //    // 删除节点
    //    if (stat != null) {
    //        zk.delete("/hifreud", stat.getVersion());
    //    }
    //}

}
