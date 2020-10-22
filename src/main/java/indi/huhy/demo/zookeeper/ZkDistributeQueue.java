package indi.huhy.demo.zookeeper;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

/**
 * ZKDistributeQueue 分布式队列实现
 * 使用zk指定的目录作为队列，子节点作为任务。
 * 生产者能往队列中添加任务，消费者能往队列中消费任务。
 * 持久节点作为队列，持久顺序节点作为任务。
 */
public class ZkDistributeQueue extends AbstractQueue<String> implements BlockingQueue<String>, java.io.Serializable {

    private static final long serialVersionUID = 1L;
    private final String zkConnUrl;
    private ZkClient zkClient;
    private final String queueRootNode;
    private static final String default_queueRootNode = "/distributeQueue";

    private String queueWriteLockNode;
    private String queueReadLockNode;
    private String queueElementNode;

    private static final String default_zkConnUrl = "localhost:2181";
    private static final int default_capacity = Integer.MAX_VALUE;
    private final int capacity;

    final Lock distributeWriteLock;
    final Lock distributeReadLock;

    public ZkDistributeQueue() {
        this(default_zkConnUrl, default_queueRootNode, default_capacity);
    }

    public ZkDistributeQueue(String zkServerUrl, String rootNodeName, int initCapacity) {
        if (zkServerUrl == null) throw new IllegalArgumentException("zkServerUrl");
        if (rootNodeName == null) throw new IllegalArgumentException("rootNodeName");
        if (initCapacity <= 0) throw new IllegalArgumentException("initCapacity");
        this.zkConnUrl = zkServerUrl;
        this.queueRootNode = rootNodeName;
        this.capacity = initCapacity;
        init();
        distributeWriteLock = new ZkDistributeImproveLock(queueWriteLockNode);
        distributeReadLock = new ZkDistributeImproveLock(queueReadLockNode);
    }

    private void init() {
        queueWriteLockNode = queueRootNode + "/writeLock";
        queueReadLockNode = queueRootNode + "/readLock";
        queueElementNode = queueRootNode + "/element";
        zkClient = new ZkClient(zkConnUrl);
        zkClient.setZkSerializer(new StringZkSerializer());
        if (!this.zkClient.exists(queueElementNode)) {
            try {
                this.zkClient.createPersistent(queueElementNode, true);
            } catch (ZkNodeExistsException e) {

            }
        }
    }

    @Override
    public String peek() {
        List<String> children = zkClient.getChildren(queueElementNode);
        if (children != null && !children.isEmpty()) {
            children = children.stream().sorted().collect(Collectors.toList());
            String firstChild = children.get(0);
            return zkClient.readData(queueElementNode + "/" + firstChild);
        }
        return null;
    }

    private void enqueue(String e) {
        zkClient.createPersistentSequential(queueElementNode + "/", e);
    }

    private boolean dequeue(String e) {
        return zkClient.delete(e);
    }

    @Override
    public String poll() {
        String firstChild = peek();
        distributeReadLock.lock();
        try {
            if (firstChild == null) {
                return null;
            }
            boolean result = dequeue(firstChild);
            if (!result) {
                return null;
            }
        } finally {
            distributeReadLock.unlock();
        }
        return firstChild;
    }

    @Override
    public boolean offer(String e) {
        checkElement(e);
        distributeWriteLock.lock();
        try {
            if (size() < capacity) {
                enqueue(e);
                return true;
            }
        } finally {
            distributeWriteLock.unlock();
        }
        return false;
    }

    @Override
    public String remove() {
        if (size() <= 0) {
            throw new IllegalDistributeQueueStateException(
                    IllegalDistributeQueueStateException.State.empty);
        }
        distributeReadLock.lock();
        String firstChild;
        try {
            firstChild = poll();
            if (firstChild == null) {
                throw new IllegalDistributeQueueStateException("移除失败");
            }
        } finally {
            distributeReadLock.unlock();
        }
        return firstChild;
    }

    @Override
    public boolean add(String e) {
        checkElement(e);
        distributeWriteLock.lock();
        try {
            if (size() >= capacity) {
                throw new IllegalDistributeQueueStateException(
                        IllegalDistributeQueueStateException.State.full);

            }
            return offer(e);
        } catch (Exception e1) {
            e1.printStackTrace();
        } finally {
            distributeWriteLock.unlock();
        }
        return false;
    }

    // 阻塞操作
    @Override
    public void put(String e) throws InterruptedException {
        checkElement(e);
        distributeWriteLock.lock();
        try {
            if (size() < capacity) {    // 容量足够
                enqueue(e);
            } else {
                waitForRemove();
                put(e);
            }
        } finally {
            distributeWriteLock.unlock();
        }
    }

    private void waitForRemove() {
        CountDownLatch cdl = new CountDownLatch(1);
        IZkChildListener listener = new IZkChildListener() {
            @Override
            public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
                if (currentChilds.size() < capacity) {    // 有任务移除，激活等待的添加操作
                    cdl.countDown();
                }
            }
        };
        zkClient.subscribeChildChanges(queueElementNode, listener);

        try {
            if (size() >= capacity) {
                cdl.await();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        zkClient.unsubscribeChildChanges(queueElementNode, listener);
    }


    @Override
    public String take() throws InterruptedException {
        distributeReadLock.lock();
        try {
            List<String> children = zkClient.getChildren(queueElementNode);
            if (children != null && !children.isEmpty()) {
                children = children.stream().sorted().collect(Collectors.toList());
                String takeChild = children.get(0);
                String childNode = queueElementNode + "/" + takeChild;
                String elementData = zkClient.readData(childNode);
                dequeue(childNode);
                return elementData;
            } else {
                waitForAdd();
                return take();
            }
        } finally {
            distributeReadLock.unlock();
        }
    }

    private void waitForAdd() {
        CountDownLatch cdl = new CountDownLatch(1);
        IZkChildListener listener = new IZkChildListener() {
            @Override
            public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
                if (currentChilds.size() > 0) {
                    cdl.countDown();
                }
            }
        };
        zkClient.subscribeChildChanges(queueElementNode, listener);

        try {
            if (size() <= 0) {
                cdl.await();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        zkClient.unsubscribeChildChanges(queueElementNode, listener);
    }

    private static void checkElement(String v) {
        if (v == null) throw new NullPointerException();
        if ("".equals(v.trim())) {
            throw new IllegalArgumentException("不能使用空格");
        }
        if (v.startsWith(" ") || v.endsWith(" ")) {
            throw new IllegalArgumentException("前后不能包含空格");
        }
    }

    @Override
    public int size() {
        return zkClient.countChildren(queueElementNode);
    }

    @Override
    public Iterator<String> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean offer(String e, long timeout, TimeUnit unit) throws InterruptedException {
        return false;
    }


    @Override
    public String poll(long timeout, TimeUnit unit) throws InterruptedException {
        return null;
    }

    @Override
    public int remainingCapacity() {
        return capacity - size();
    }

    @Override
    public int drainTo(Collection<? super String> c) {
        return drainTo(c, size());
    }


    @Override
    public int drainTo(Collection<? super String> c, int maxElements) {
        if (c == null) {
            throw new NullPointerException();
        }
        int transferredSize = 0;
        List<String> children = zkClient.getChildren(queueElementNode);
        if (children != null && !children.isEmpty()) {
            List<String> removeElements = children.stream().sorted().limit(maxElements).collect(Collectors.toList());
            transferredSize = removeElements.size();
            c.addAll(removeElements);
            removeElements.forEach((e) -> {
                zkClient.delete(e);
            });
        }
        return transferredSize;
    }
}

