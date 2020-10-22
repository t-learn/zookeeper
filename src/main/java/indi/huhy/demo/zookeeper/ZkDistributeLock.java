package indi.huhy.demo.zookeeper;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class ZkDistributeLock implements Lock {

    private final String lockPath;
    private final ZkClient client;

    public ZkDistributeLock(String lockPath) {
        if (lockPath == null || lockPath.trim().equals("")) {
            throw new IllegalArgumentException("path can not be null!");
        }
        this.lockPath = lockPath;

        client = new ZkClient("localhost:2181");
        client.setZkSerializer(new StringZkSerializer());
    }

    @Override
    public boolean tryLock() {
        try {
            client.createEphemeral(lockPath);
        } catch (ZkNodeExistsException e) {
            return false;
        }
        return true;
    }

    @Override
    public void unlock() {
        client.delete(lockPath);
    }

    @Override
    public void lock() {
        if (!tryLock()) {
            waitForLock();
            lock();
        }

    }

    private void waitForLock() {
        CountDownLatch cdl = new CountDownLatch(1);

        IZkDataListener listener = new IZkDataListener() {
            @Override
            public void handleDataDeleted(String dataPath) throws Exception {
                cdl.countDown();
            }

            @Override
            public void handleDataChange(String dataPath, Object data)
                    throws Exception {
            }
        };
        client.subscribeDataChanges(lockPath, listener);
        if (this.client.exists(lockPath)) {
            try {
                cdl.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        client.unsubscribeDataChanges(lockPath, listener);
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit)
            throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

}
