package indi.huhy.demo.zookeeper;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class ZkDistributeImproveLock implements Lock {

    /*
     * 利用临时顺序节点来实现分布式锁
     * 获取锁：取排队号（创建自己的临时顺序节点），然后判断自己是否是最小号，如是，则获得锁；不是，则注册前一节点的watcher,阻塞等待
     * 释放锁：删除自己创建的临时顺序节点
     */
    private final String lockPath;
    private final ZkClient client;

    private final ThreadLocal<String> currentPath = new ThreadLocal<>();
    private final ThreadLocal<String> beforePath = new ThreadLocal<>();
    private final ThreadLocal<Integer> reenterCount = ThreadLocal.withInitial(() -> 0);

    public ZkDistributeImproveLock(String lockPath) {
        if (lockPath == null || lockPath.trim().equals("")) {
            throw new IllegalArgumentException("path can not be null!");
        }
        this.lockPath = lockPath;
        client = new ZkClient("localhost:2181");
        client.setZkSerializer(new StringZkSerializer());
        if (!this.client.exists(lockPath)) {
            try {
                this.client.createPersistent(lockPath, true);
            } catch (ZkNodeExistsException ignored) {
            }
        }
    }

    @Override
    public boolean tryLock() {
        if (this.currentPath.get() == null || !client.exists(this.currentPath.get())) {
            String node = this.client.createEphemeralSequential(lockPath + "/", "locked");
            currentPath.set(node);
            reenterCount.set(0);
        }

        List<String> children = this.client.getChildren(lockPath);
        Collections.sort(children);
        if (currentPath.get().equals(lockPath + "/" + children.get(0))) {
            reenterCount.set(reenterCount.get() + 1);
            return true;
        } else {
            int curIndex = children.indexOf(currentPath.get().substring(lockPath.length() + 1));
            String node = lockPath + "/" + children.get(curIndex - 1);
            beforePath.set(node);
        }
        return false;
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void lock() {
        if (!tryLock()) {
            waitForLock();
            lock();
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    private void waitForLock() {
        CountDownLatch cdl = new CountDownLatch(1);
        IZkDataListener listener = new IZkDataListener() {
            @Override
            public void handleDataDeleted(String dataPath) throws Exception {
                cdl.countDown();
            }

            @Override
            public void handleDataChange(String dataPath, Object data) throws Exception {
            }
        };

        client.subscribeDataChanges(this.beforePath.get(), listener);
        if (this.client.exists(this.beforePath.get())) {
            try {
                cdl.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        client.unsubscribeDataChanges(this.beforePath.get(), listener);
    }

    @Override
    public void unlock() {
        if (reenterCount.get() > 1) {
            reenterCount.set(reenterCount.get() - 1);
            return;
        }
        if (this.currentPath.get() != null) {
            this.client.delete(this.currentPath.get());
            this.currentPath.set(null);
            this.reenterCount.set(0);
        }
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }
}
