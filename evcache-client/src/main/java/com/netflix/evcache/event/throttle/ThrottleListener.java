package com.netflix.evcache.event.throttle;

import java.util.Set;
import java.util.function.Supplier;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.evcache.EVCache.Call;
import com.netflix.evcache.event.EVCacheEvent;
import com.netflix.evcache.event.EVCacheEventListener;
import com.netflix.evcache.pool.EVCacheClientPoolManager;

/**
 * <p>
 * To enable throttling on operations the set the below property
 *      <code>EVCacheThrottler.throttle.operations=true</code>
 * </p>
 * <p>
 * To throttle all operations specified in {@link Call} then add the {@link Call} (separated by comma(,)) to the below property.<br>
 *  <code>&lt;EVCache appName&gt;.throttle.calls=&lt;comma separated list of calls&gt;</code><br>
 *  <br>
 *  EX: To throttle {@link Call.GET} and {@link Call.DELETE} operations for EVCACHE_CRS set the below property
 *  <code>EVCACHE_CRS.throttle.calls=GET,DELETE</code>
 * 
 * @author smadappa
 */

@Singleton
public class ThrottleListener implements EVCacheEventListener {

    private static final Logger log = LoggerFactory.getLogger(ThrottleListener.class);
    private final Supplier<Boolean> enableThrottleOperations;
    private final EVCacheClientPoolManager poolManager;

    @Inject 
    public ThrottleListener(EVCacheClientPoolManager poolManager) {
        this.poolManager = poolManager;
        this.enableThrottleOperations = poolManager.getCacheConfig().isEnableThrottleOperations();
        poolManager.getCacheConfig().addCallback(enableThrottleOperations, this::setupListener);
        if(this.enableThrottleOperations.get()) setupListener();
    }

    private void setupListener() {
        if(enableThrottleOperations.get()) {
            poolManager.addEVCacheEventListener(this);
        } else {
            poolManager.removeEVCacheEventListener(this);
        }
    }

    @Override
    public void onStart(final EVCacheEvent e) {
    }

    @Override
    public boolean onThrottle(final EVCacheEvent e) {
        if(!enableThrottleOperations.get()) return false;

        final String appName = e.getAppName();
        Set<String> throttleCalls = poolManager.getCacheConfig().getClusterConfig(appName).getThrottlerConfig().getThrottleCalls().get();
        if(!throttleCalls.isEmpty() && throttleCalls.contains(e.getCall().name())) {
            if(log.isDebugEnabled()) log.debug("Call : " + e.getCall() + " is throttled");
            return true;
        }
        return false;
    }

    @Override
    public void onComplete(EVCacheEvent e) {
    }

    @Override
    public void onError(EVCacheEvent e, Throwable t) {
    }

}