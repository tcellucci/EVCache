package com.netflix.evcache.metrics;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.evcache.EVCache.Call;
import com.netflix.servo.monitor.StatsTimer;
import com.netflix.servo.monitor.StepCounter;

@edu.umd.cs.findbugs.annotations.SuppressFBWarnings("REC_CATCH_EXCEPTION")
public class EVCacheMetrics implements EVCacheMetricsMBean, Stats {
    private static final Logger log = LoggerFactory.getLogger(EVCacheMetrics.class);

    private final String appName, cacheName;
    private final EVCacheMetricsFactory cacheMetricsFactory;
    private StepCounter getCallsCounter, bulkCallsCounter, bulkHitsCounter, getHitsCounter, setCallsCounter, addCallsCounter, replaceCallCounter, delCallsCounter, incrCounter, decrCounter;
    private StepCounter bulkMissCounter, getMissCounter;
    private StatsTimer getDuration, bulkDuration, appendOrAddDuration, appendDuration;

    EVCacheMetrics(EVCacheMetricsFactory cacheMetricsFactory, final String appName, String _cacheName) {
    	this.cacheMetricsFactory = cacheMetricsFactory;
        this.appName = appName;
        this.cacheName = (_cacheName == null) ? "" : _cacheName;

        setupMonitoring(appName, cacheName);
    }

    public void operationCompleted(Call op, long duration) {
        if (op == Call.GET || op == Call.GET_AND_TOUCH) {
            getCallCounter().increment();
            getGetCallDuration().record(duration);
        } else if (op == Call.SET) {
            getSetCallCounter().increment();
        } else if (op == Call.REPLACE) {
            getReplaceCallCounter().increment();
        } else if (op == Call.DELETE) {
            getDeleteCallCounter().increment();
        } else if (op == Call.BULK) {
            getBulkCounter().increment();
            getBulkCallDuration().record(duration);
        } else if (op == Call.APPEND_OR_ADD) {
            getAppendOrAddDuration().record(duration);
        } else if (op == Call.ADD) {
            getAddCallCounter().increment();
        } else if (op == Call.APPEND) {
            getAppendDuration().record(duration);
        } else if (op == Call.INCR) {
            getIncrCounter().increment();
        } else if (op == Call.DECR) {
            getDecrCounter().increment();
        }
    }

    private void setupMonitoring(String _appName, String _cacheName) {
        try {
            String mBeanName = "com.netflix.evcache:Group=" + _appName + ",SubGroup=AtlasStats";
            if (_cacheName != null) mBeanName = mBeanName + ",SubSubGroup=" + _cacheName;
            final ObjectName mBeanObj = ObjectName.getInstance(mBeanName);
            final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
            if (mbeanServer.isRegistered(mBeanObj)) {
                if (log.isDebugEnabled()) log.debug("MBEAN with name " + mBeanObj + " has been registered. Will unregister the previous instance and register a new one.");
                mbeanServer.unregisterMBean(mBeanObj);
            }
            mbeanServer.registerMBean(this, mBeanObj);
            if (log.isDebugEnabled()) log.debug("MBEAN with name " + mBeanObj + " has been registered.");
        } catch (Exception e) {
            if (log.isDebugEnabled()) log.debug(e.getMessage(), e);
        }
    }

    private StepCounter getCallCounter() {
        if (this.getCallsCounter != null) return this.getCallsCounter;

        this.getCallsCounter = cacheMetricsFactory.getStepCounter(appName, cacheName, "GetCall");
        getHitCounter();

        return getCallsCounter;
    }

    private StepCounter getHitCounter() {
        if (this.getHitsCounter != null) return this.getHitsCounter;

        this.getHitsCounter = cacheMetricsFactory.getStepCounter(appName, cacheName, "GetHit");
        return getHitsCounter;
    }

    private StepCounter getMissCounter() {
        if (this.getMissCounter != null) return this.getMissCounter;

        this.getMissCounter = cacheMetricsFactory.getStepCounter(appName, cacheName, "GetMiss");
        return getMissCounter;
    }

    private StepCounter getBulkCounter() {
        if (this.bulkCallsCounter != null) return this.bulkCallsCounter;

        this.bulkCallsCounter = cacheMetricsFactory.getStepCounter(appName, cacheName, "BulkCall");
        return bulkCallsCounter;
    }

    private StepCounter getBulkHitCounter() {
        if(this.bulkHitsCounter != null) return this.bulkHitsCounter;

        this.bulkHitsCounter = cacheMetricsFactory.getStepCounter(appName, cacheName, "BulkHit");
        return bulkHitsCounter;
    }

    private StepCounter getBulkMissCounter() {
        if(this.bulkMissCounter != null) return this.bulkMissCounter;

        this.bulkMissCounter = cacheMetricsFactory.getStepCounter(appName, cacheName, "BulkMiss");
        return bulkMissCounter;
    }

    
    private StepCounter getAddCallCounter() {
        if (this.addCallsCounter != null) return this.addCallsCounter;

        this.addCallsCounter = cacheMetricsFactory.getStepCounter(appName, cacheName, "AddCall");
        return addCallsCounter;
    }

    private StepCounter getSetCallCounter() {
        if (this.setCallsCounter != null) return this.setCallsCounter;

        this.setCallsCounter = cacheMetricsFactory.getStepCounter(appName, cacheName, "SetCall");
        return setCallsCounter;
    }

    private StepCounter getReplaceCallCounter() {
        if (this.replaceCallCounter != null) return this.replaceCallCounter;

        this.replaceCallCounter = cacheMetricsFactory.getStepCounter(appName, cacheName, "ReplaceCall");
        return replaceCallCounter;
    }

    private StepCounter getDeleteCallCounter() {
        if (this.delCallsCounter != null) return this.delCallsCounter;

        this.delCallsCounter = cacheMetricsFactory.getStepCounter(appName, cacheName, "DeleteCall");
        return delCallsCounter;
    }

    private StatsTimer getAppendOrAddDuration() {
        if (appendOrAddDuration != null) return appendOrAddDuration;

        this.appendOrAddDuration = cacheMetricsFactory.getStatsTimer(appName, cacheName, "LatencyAppendOrAdd");
        return appendOrAddDuration;
    }
    
    private StatsTimer getAppendDuration() {
        if (appendDuration != null) return appendDuration;

        this.appendDuration = cacheMetricsFactory.getStatsTimer(appName, cacheName, "LatencyAppend");
        return appendDuration;
    }
    
    private StatsTimer getGetCallDuration() {
        if (getDuration != null) return getDuration;

        this.getDuration = cacheMetricsFactory.getStatsTimer(appName, cacheName, "LatencyGet");
        return getDuration;
    }

    private StatsTimer getBulkCallDuration() {
        if (bulkDuration != null) return bulkDuration;

        this.bulkDuration = cacheMetricsFactory.getStatsTimer(appName, cacheName, "LatencyBulk");
        return bulkDuration;
    }

    public long getGetCalls() {
        return getCallCounter().getValue().longValue();
    }

    public long getCacheHits() {
        return getHitCounter().getValue().longValue();
    }

    public long getCacheMiss() {
        return getMissCounter().getValue().longValue();
    }

    public long getBulkCalls() {
        return getBulkCounter().getValue().longValue();
    }

    public long getBulkHits() {
        return getBulkHitCounter().getValue().longValue();
    }

    public long getBulkMiss() {
        return getBulkMissCounter().getValue().longValue();
    }

    public long getSetCalls() {
        return getSetCallCounter().getValue().longValue();
    }

    public void cacheHit(Call call) {
        if (call == Call.BULK) {
            this.getBulkHitCounter().increment();
        } else {
            this.getHitCounter().increment();
        }
    }

    public void cacheMiss(Call call) {
        if (call == Call.BULK) {
            this.getBulkMissCounter().increment();
        } else {
            this.getMissCounter().increment();
        }
    }

    private StepCounter getIncrCounter() {
        if (this.incrCounter != null) return this.incrCounter;

        this.incrCounter = cacheMetricsFactory.getStepCounter(appName, cacheName, "IncrCall");

        return incrCounter;
    }

    private StepCounter getDecrCounter() {
        if (this.decrCounter != null) return this.decrCounter;

        this.decrCounter = cacheMetricsFactory.getStepCounter(appName, cacheName, "DecrCall");

        return decrCounter;
    }

    public long getGetDuration() {
        return getGetCallDuration().getValue().longValue();
    }

    public long getBulkDuration() {
        return getBulkCallDuration().getValue().longValue();
    }

    public String toString() {
        return "EVCacheMetrics [ AppName=" + appName + ",  CachePrefix=" + cacheName + ", getCalls=" + getCallCounter() + ", bulkCalls="
                + getBulkCounter() + ", setCalls=" + getSetCallCounter() + ", cacheHits=" + getHitCounter() + ", cacheMiss=" + getMissCounter() 
                + ", bulkHits=" + getBulkHitCounter() + ", bulkMiss=" + getBulkMissCounter() + ", deleteCalls=" + getDeleteCallCounter()
                + ", getDuration=" + getGetCallDuration() + ", bulkDuration=" + getBulkCallDuration() + ", replaceCalls=" + getReplaceCallCounter() + "]";
    }

    public double getHitRate() {
        return (getCacheHits() / getGetCalls()) * 100;
    }

    public double getBulkHitRate() {
        return (getBulkHits() / getBulkCalls()) * 100;
    }
}