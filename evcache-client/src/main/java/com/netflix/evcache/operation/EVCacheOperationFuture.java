package com.netflix.evcache.operation;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.evcache.EVCacheGetOperationListener;
import com.netflix.evcache.metrics.EVCacheMetricsFactory;
import com.netflix.evcache.pool.ServerGroup;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.tag.BasicTagList;
import com.netflix.servo.tag.TagList;
import com.sun.management.GcInfo;

import net.spy.memcached.MemcachedConnection;
import net.spy.memcached.internal.CheckedOperationTimeoutException;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.protocol.binary.EVCacheNodeImpl;
import rx.Scheduler;
import rx.Single;
import rx.functions.Action0;

/**
 * Managed future for operations.
 *
 * <p>
 * From an OperationFuture, application code can determine if the status of a
 * given Operation in an asynchronous manner.
 *
 * <p>
 * If for example we needed to update the keys "user:<userid>:name",
 * "user:<userid>:friendlist" because later in the method we were going to
 * verify the change occurred as expected interacting with the user, we can fire
 * multiple IO operations simultaneously with this concept.
 *
 * @param <T>
 *            Type of object returned from this future.
 */
@edu.umd.cs.findbugs.annotations.SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_HAS_CHECKED")
public class EVCacheOperationFuture<T> extends OperationFuture<T> {

    private Logger log = LoggerFactory.getLogger(EVCacheOperationFuture.class);

    private final CountDownLatch latch;
    private final AtomicReference<T> objRef;
    private Operation op;
    private final String appName;
    private final ServerGroup serverGroup;
    private final String key;

    public EVCacheOperationFuture(String k, CountDownLatch l, AtomicReference<T> oref, long opTimeout, ExecutorService service, String appName, ServerGroup serverGroup) {
        super(k, l, oref, opTimeout, service);
        this.latch = l;
        this.objRef = oref;
        this.appName = appName;
        this.serverGroup = serverGroup;
        this.key = k;
    }

    public Operation getOperation() {
        return this.op;
    }

    public void setOperation(Operation to) {
        this.op = to;
        super.setOperation(to);
    }

    public String getApp() {
        return appName;
    }

    public String getKey() {
        return key;
    }

    public String getZone() {
        return (serverGroup == null ? "NA" : serverGroup.getZone());
    }

    public ServerGroup getServerGroup() {
        return serverGroup;
    }

    public EVCacheOperationFuture<T> addListener(EVCacheGetOperationListener<T> listener) {
        super.addToListeners(listener);
        return this;
    }

    public EVCacheOperationFuture<T> removeListener(EVCacheGetOperationListener<T> listener) {
        super.removeFromListeners(listener);
        return this;
    }

    /**
     * Get the results of the given operation.
     *
     * As with the Future interface, this call will block until the results of
     * the future operation has been received.
     * 
     * Note: If we detect there was GC pause and our operation was caught in
     * between we wait again to see if we will be successful. This is effective
     * as the timeout we specify is very low.
     *
     * @param duration
     *            amount of time to wait
     * @param units
     *            unit of time to wait
     * @param if
     *            exeception needs to be thrown of null returned on a failed
     *            operation
     * @param has
     *            zone fallback
     * @return the operation results of this OperationFuture
     * @throws InterruptedException
     * @throws TimeoutException
     * @throws ExecutionException
     */
    public T get(long duration, TimeUnit units, boolean throwException, boolean hasZF) throws InterruptedException, TimeoutException, ExecutionException {
        final long startTime = System.currentTimeMillis();
        boolean status = latch.await(duration, units);
        if (!status) {
            boolean gcPause = false;
            final RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();
            final long vmStartTime = runtimeBean.getStartTime();
            final List<GarbageCollectorMXBean> gcMXBeans = ManagementFactory.getGarbageCollectorMXBeans();
            for (GarbageCollectorMXBean gcMXBean : gcMXBeans) {
                if (gcMXBean instanceof com.sun.management.GarbageCollectorMXBean) {
                    final GcInfo lastGcInfo = ((com.sun.management.GarbageCollectorMXBean) gcMXBean).getLastGcInfo();

                    // If no GCs, there was no pause due to GC.
                    if (lastGcInfo == null) {
                        continue;
                    }

                    final long gcStartTime = lastGcInfo.getStartTime() + vmStartTime;
                    if (gcStartTime > startTime) {
                        gcPause = true;
                        final long gcDuration = lastGcInfo.getDuration();
                        final long pauseDuration = System.currentTimeMillis() - gcStartTime;
                        if (log.isDebugEnabled()) log.debug("Total GC duration = " + gcDuration + " msec.");
                        if (log.isDebugEnabled()) log.debug("Total pause duration due to gc event = " + pauseDuration + " msec.");
                        break;
                    }
                }
            }
            if (!gcPause && log.isDebugEnabled()) {
                log.debug("Total pause duration due to NON-GC event = " + (System.currentTimeMillis() - startTime) + " msec.");
            }
            // redo the same op once more since there was a chance of gc pause
            status = latch.await(duration, units);
            final long pauseDuration = System.currentTimeMillis() - startTime;
            EVCacheMetricsFactory.getStatsTimer(appName, serverGroup, "Pause-get", gcPause ? "GC":"Scheduling", status ? "Succcess":"Fail").record(pauseDuration);
        }

        if (log.isDebugEnabled()) log.debug("Retry status : " + status);
        if (!status) {
            // whenever timeout occurs, continuous timeout counter will increase by 1.
            MemcachedConnection.opTimedOut(op);
            if (op != null) op.timeOut();
            TagList tags = null;
            if(op.getHandlingNode() instanceof EVCacheNodeImpl) {
            	tags = ((EVCacheNodeImpl)op.getHandlingNode()).getBaseTags();
            } else {
            	tags = BasicTagList.of(DataSourceType.COUNTER);
            	
            }
            if (!hasZF) EVCacheMetricsFactory.getCounter(appName, null, serverGroup.getName(), appName + "-get-CheckedOperationTimeout", tags).increment();
            if (throwException) {
                throw new CheckedOperationTimeoutException("Timed out waiting for operation", op);
            }
        } else {
            // continuous timeout counter will be reset
            MemcachedConnection.opSucceeded(op);
        }

        if (op != null && op.hasErrored()) {
            if (throwException) {
                throw new ExecutionException(op.getException());
            }
        }
        if (isCancelled()) {
            TagList tags = null;
            if(op.getHandlingNode() instanceof EVCacheNodeImpl) {
            	tags = ((EVCacheNodeImpl)op.getHandlingNode()).getBaseTags();
            } else {
            	tags = BasicTagList.of(DataSourceType.COUNTER);
            	
            }
            if (!hasZF) EVCacheMetricsFactory.getCounter(appName, null, serverGroup.getName(), appName + "-get-Cancelled", tags).increment();
            if (throwException) {
                throw new ExecutionException(new CancellationException("Cancelled"));
            }
        }
        if (op != null && op.isTimedOut()) {
//            if (!hasZF) EVCacheMetricsFactory.getCounter(appName, null, serverGroup.getName(), appName + "-get-Error", DataSourceType.COUNTER).increment();
            if (throwException) {
                throw new ExecutionException(new CheckedOperationTimeoutException("Operation timed out.", op));
            }
        }
        return objRef.get();
    }

    public Single<T> observe() {
        return Single.create(subscriber ->
            addListener((EVCacheGetOperationListener<T>) future -> {
                try {
                    subscriber.onSuccess(get());
                } catch (Throwable e) {
                    subscriber.onError(e);
                }
            })
        );
    }

    public Single<T> get(long duration, TimeUnit units, boolean throwException, boolean hasZF, Scheduler scheduler) {
        return observe().timeout(duration, units, Single.create(subscriber -> {
            // whenever timeout occurs, continuous timeout counter will increase by 1.
            MemcachedConnection.opTimedOut(op);
            if (op != null) op.timeOut();
            if (!hasZF) EVCacheMetricsFactory.getCounter(appName, null, serverGroup.getName(), appName + "-get-CheckedOperationTimeout", DataSourceType.COUNTER).increment();
            if (throwException) {
                subscriber.onError(new CheckedOperationTimeoutException("Timed out waiting for operation", op));
            } else {
                if (isCancelled()) {
                    if (hasZF) EVCacheMetricsFactory.getCounter(appName, null, serverGroup.getName(), appName + "-get-Cancelled", DataSourceType.COUNTER).increment();
                }
                subscriber.onSuccess(objRef.get());
            }
        }), scheduler).doAfterTerminate(new Action0() {
            @Override
            public void call() {
                
            }
        }
        );
    }

    public void signalComplete() {
        super.signalComplete();
    }
    
    /**
     * Cancel this operation, if possible.
     *
     * @param ign not used
     * @deprecated
     * @return true if the operation has not yet been written to the network
     */
    public boolean cancel(boolean ign) {
        if(log.isDebugEnabled()) log.debug("Operation cancelled", new Exception());
      return super.cancel(ign);
    }

    /**
     * Cancel this operation, if possible.
     *
     * @return true if the operation has not yet been written to the network
     */
    public boolean cancel() {
        if(log.isDebugEnabled()) log.debug("Operation cancelled", new Exception());
        return super.cancel();
    }
    

}