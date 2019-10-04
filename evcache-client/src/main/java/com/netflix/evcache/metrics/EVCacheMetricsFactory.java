package com.netflix.evcache.metrics;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.netflix.config.DynamicIntProperty;
import com.netflix.evcache.EVCache.Call;
import com.netflix.evcache.pool.ServerGroup;
import com.netflix.evcache.util.EVCacheConfig;
import com.netflix.servo.DefaultMonitorRegistry;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.monitor.BasicCounter;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.DoubleGauge;
import com.netflix.servo.monitor.LongGauge;
import com.netflix.servo.monitor.Monitor;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.monitor.Monitors;
import com.netflix.servo.monitor.MonitorConfig.Builder;
import com.netflix.servo.monitor.StatsTimer;
import com.netflix.servo.monitor.StepCounter;
import com.netflix.servo.monitor.Timer;
import com.netflix.servo.stats.StatsConfig;
import com.netflix.servo.tag.BasicTagList;
import com.netflix.servo.tag.Tag;
import com.netflix.servo.tag.TagList;
import com.netflix.servo.tag.Tags;
import com.netflix.spectator.api.DistributionSummary;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Spectator;
import com.netflix.servo.tag.BasicTag;

@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = { "NF_LOCAL_FAST_PROPERTY",
        "PMB_POSSIBLE_MEMORY_BLOAT" }, justification = "Creates only when needed")
public final class EVCacheMetricsFactory {
    private static final Map<String, Stats> statsMap = new ConcurrentHashMap<String, Stats>();
    private static final Map<String, Monitor<?>> monitorMap = new ConcurrentHashMap<String, Monitor<?>>();
    private static final Map<String, DistributionSummary> distributionSummaryMap = new ConcurrentHashMap<String, DistributionSummary>();
    private static final Lock writeLock = (new ReentrantReadWriteLock()).writeLock();
    private static final Map<String, Timer> timerMap = new HashMap<String, Timer>();
    private static final DynamicIntProperty sampleSize = EVCacheConfig.getInstance().getDynamicIntProperty("EVCache.metrics.sample.size", 100);
    public static final Tag OWNER = Tags.newTag("owner", "evcache");

    public static Operation getOperation(String name) {
        return getOperation(name, null, null, Operation.TYPE.MILLI);
    }

    public static Operation getOperation(String name, Call op, Stats stats) {
        return getOperation(name, op, stats, Operation.TYPE.MILLI);
    }

    public static Operation getOperation(String name, Call op, Stats stats, Operation.TYPE type) {
        final Operation operation = new EVCacheOperation(name, op, stats, type);
        operation.start();
        return operation;
    }

    public static Stats getStats(String appName, String cacheName) {
        final String key = (cacheName == null) ? new StringBuilder(128).append(appName).append(":NA").toString() : new StringBuilder(128).append(appName ).append(":").append(cacheName).toString();
        Stats metrics = statsMap.get(key);
        if (metrics != null) return metrics;
        writeLock.lock();
        try {
            if (statsMap.containsKey(key)) {
                metrics = statsMap.get(key);
            } else {
                statsMap.put(key, metrics = new EVCacheMetrics(appName, cacheName));
            }
        } finally {
            writeLock.unlock();
        }
        return metrics;
    }

    public static Map<String, Stats> getAllMetrics() {
        return statsMap;
    }

    public static Map<String, Monitor<?>> getAllMonitor() {
        return monitorMap;
    }
    
    public static Map<String, DistributionSummary> getAllDistributionSummaryMap() {
        return distributionSummaryMap;
    }
    

    public static LongGauge getLongGauge(String name) {
        LongGauge gauge = (LongGauge) monitorMap.get(name);
        if (gauge == null) {
            writeLock.lock();
            try {
                if (monitorMap.containsKey(name)) {
                    gauge = (LongGauge) monitorMap.get(name);
                } else {
                    gauge = new LongGauge(MonitorConfig.builder(name).withTag(OWNER).build());
                    monitorMap.put(name, gauge);
                    DefaultMonitorRegistry.getInstance().register(gauge);
                }
            } finally {
                writeLock.unlock();
            }
        }
        return gauge;
    }

    public static LongGauge getLongGauge(String cName, TagList tag) {
        final String name = new StringBuilder(128).append(cName).append(tag.toString()).toString();
        LongGauge gauge = (LongGauge) monitorMap.get(name);
        if (gauge == null) {
            writeLock.lock();
            try {
                if (monitorMap.containsKey(name)) {
                    gauge = (LongGauge) monitorMap.get(name);
                } else {
                    gauge = new LongGauge(MonitorConfig.builder(cName).withTags(tag).withTag(OWNER).build());
                    monitorMap.put(name, gauge);
                    DefaultMonitorRegistry.getInstance().register(gauge);
                }
            } finally {
                writeLock.unlock();
            }
        }
        return gauge;
    }

    public static DoubleGauge getDoubleGauge(String cName, TagList tag) {
        final String name = new StringBuilder(128).append(cName).append(tag.toString()).append(".DoubleGauge").toString();
        DoubleGauge gauge = (DoubleGauge) monitorMap.get(name);
        if (gauge == null) {
            writeLock.lock();
            try {
                if (monitorMap.containsKey(name)) {
                    gauge = (DoubleGauge) monitorMap.get(name);
                } else {
                    gauge = new DoubleGauge(MonitorConfig.builder(cName).withTags(tag).withTag(OWNER).build());
                    monitorMap.put(name, gauge);
                    DefaultMonitorRegistry.getInstance().register(gauge);
                }
            } finally {
                writeLock.unlock();
            }
        }
        return gauge;
    }

    public static Counter getCounter(String cName, Tag tag) {
        if (tag == null) return getCounter(cName);
        final String name = new StringBuilder(128).append(cName).append(tag.tagString()).toString();
        Counter counter = (Counter) monitorMap.get(name);
        if (counter == null) {
            writeLock.lock();
            try {
                if (monitorMap.containsKey(name)) {
                    counter = (Counter) monitorMap.get(name);
                } else {
                    counter = new BasicCounter(MonitorConfig.builder(cName).withTag(OWNER).withTag(tag).build());
                    monitorMap.put(name, counter);
                    DefaultMonitorRegistry.getInstance().register(counter);
                }
            } finally {
                writeLock.unlock();
            }
        }
        return counter;
    }

    public static Counter getCounter(String cName, TagList tag) {
        final String name = new StringBuilder(128).append(cName).append(tag.toString()).toString();
        Counter counter = (Counter) monitorMap.get(name);
        if (counter == null) {
            writeLock.lock();
            try {
                if (monitorMap.containsKey(name)) {
                    counter = (Counter) monitorMap.get(name);
                } else {
                    counter = new BasicCounter(MonitorConfig.builder(cName).withTag(OWNER).withTags(tag).build());
                    monitorMap.put(name, counter);
                    DefaultMonitorRegistry.getInstance().register(counter);
                }
            } finally {
                writeLock.unlock();
            }
        }
        return counter;
    }

    public static Counter getCounter(String name) {
        return getCounter(name, DataSourceType.COUNTER);
    }

    public static void increment(String name) {
        final Counter counter = getCounter(name);
        counter.increment();
    }

    public static void increment(String appName, String cacheName, String metricName) {
        final Counter counter = getCounter(appName, cacheName, null, metricName, DataSourceType.COUNTER);
        counter.increment();
    }

    public static void increment(String appName, String cacheName, String serverGroupName, String metricName) {
        final Counter counter = getCounter(appName, cacheName, serverGroupName, metricName, DataSourceType.COUNTER);
        counter.increment();
    }

    public static Counter getCounter(String appName, String cacheName, String metricName, Tag tag) {
        return getCounter(appName, cacheName, null, metricName, DataSourceType.COUNTER);
    }
    
    public static Counter getCounter(String appName, String cacheName, String serverGroupName, String metricName, Tag tag) {
        final TagList tags = tag != null ? BasicTagList.of(tag) : null;
        return getCounter(appName, cacheName, serverGroupName, metricName, tags);
    }

    public static Counter getCounter(String appName, String cacheName, String serverGroupName, String metricName, TagList tags) {
    	if(tags == null) tags = BasicTagList.of("APP", appName, DataSourceType.COUNTER.getKey(), DataSourceType.COUNTER.getValue());
        final String name =  new StringBuilder(128).append(appName).append((cacheName != null ? cacheName : "")).append((serverGroupName != null ? serverGroupName : "")).append(metricName).append(tags.toString()).toString();
        Counter counter = (Counter) monitorMap.get(name);
        if (counter == null) {
            if (cacheName != null && cacheName.length() > 0 && !tags.containsKey("CACHE")) {
                tags = BasicTagList.concat(tags, new BasicTag("CACHE", cacheName));
            }
            if(serverGroupName != null && serverGroupName.length() > 0 && !tags.containsKey("ServerGroup")) {
                tags = BasicTagList.concat(tags, new BasicTag("ServerGroup", serverGroupName));
            }
            if(!tags.containsKey(DataSourceType.COUNTER.getKey())) {
                tags = BasicTagList.concat(tags, DataSourceType.COUNTER);
            }
            writeLock.lock();
            try {
                if (monitorMap.containsKey(name)) {
                    counter = (Counter) monitorMap.get(name);
                } else {
                    counter = new BasicCounter(MonitorConfig.builder(metricName).withTag(OWNER).build().withAdditionalTags(tags));
                    monitorMap.put(name, counter);
                    DefaultMonitorRegistry.getInstance().register(counter);
                }
            } finally {
                writeLock.unlock();
            }
        }
        return counter;
    }

    public static StepCounter getStepCounter(String appName, String cacheName, String metric) {
        final String metricName = getMetricName(appName, null, metric);
        final String name =  new StringBuilder(128).append(metricName).append((cacheName == null ? "" : "-" + cacheName + "-")).append("type=StepCounter").toString();
        final StepCounter counter = (StepCounter) monitorMap.get(name);
        if (counter != null) return counter;
        writeLock.lock();
        try {
            if (monitorMap.containsKey(name))
                return (StepCounter) monitorMap.get(name);
            else {
                final StepCounter _counter = new StepCounter(getMonitorConfig(metricName, appName, cacheName, metric));
                monitorMap.put(name, _counter);
                DefaultMonitorRegistry.getInstance().register(_counter);
                return _counter;
            }
        } finally {
            writeLock.unlock();
        }

    }

    public static StatsTimer getStatsTimer(String appName, String cacheName, String metric) {
        final String metricName = getMetricName(appName, null, metric);
        final String name = new StringBuilder(128).append(metricName).append((cacheName == null ? "" : "-" + cacheName + "-")).append("type=StatsTimer").toString();
        final StatsTimer duration = (StatsTimer) monitorMap.get(name);
        if (duration != null) return duration;

        writeLock.lock();
        try {
            if (monitorMap.containsKey(name))
                return (StatsTimer) monitorMap.get(name);
            else {
                final StatsConfig statsConfig = new StatsConfig.Builder().withPercentiles(new double[] { 95, 99 })
                        .withPublishMax(true).withPublishMin(true)
                        .withPublishMean(true).withPublishCount(true).withSampleSize(sampleSize.get()).build();
                final StatsTimer _duration = new StatsTimer(getMonitorConfig(metricName, appName, cacheName, metric),
                        statsConfig, TimeUnit.MILLISECONDS);
                monitorMap.put(name, _duration);
                DefaultMonitorRegistry.getInstance().register(_duration);
                return _duration;
            }
        } finally {
            writeLock.unlock();
        }
    }

    public static StatsTimer getStatsTimer(String appName, ServerGroup serverGroup, String metric) {
        final String serverGroupName = (serverGroup != null ? serverGroup.getName() : "");
        final String metricName = getMetricName(appName, null, metric);
        final String name = new StringBuilder(128).append( metricName).append(serverGroupName).append("type=StatsTimer").toString();
        final StatsTimer duration = (StatsTimer) monitorMap.get(name);
        if (duration != null) return duration;

        writeLock.lock();
        try {
            if (monitorMap.containsKey(name))
                return (StatsTimer) monitorMap.get(name);
            else {
                final StatsConfig statsConfig = new StatsConfig.Builder().withPercentiles(new double[] { 95, 99 })
                        .withPublishMax(true).withPublishMin(true)
                        .withPublishMean(true).withPublishCount(true).withSampleSize(sampleSize.get()).build();
                final StatsTimer _duration = new StatsTimer(getMonitorConfig(metricName, appName, null, serverGroupName, metric), statsConfig, TimeUnit.MILLISECONDS);
                monitorMap.put(name, _duration);
                DefaultMonitorRegistry.getInstance().register(_duration);
                return _duration;
            }
        } finally {
            writeLock.unlock();
        }
    }

    public static StatsTimer getStatsTimer(String appName, ServerGroup serverGroup, String metric, String reason, String status) {
        final String serverGroupName = (serverGroup != null ? serverGroup.getName() : "");
        final String metricName = getMetricName(appName, null, metric);
        final String name = new StringBuilder(128).append(metricName).append(serverGroupName).append(reason).append(status).append("type=StatsTimer").toString();
        final StatsTimer duration = (StatsTimer) monitorMap.get(name);
        if (duration != null) return duration;

        writeLock.lock();
        try {
            if (monitorMap.containsKey(name))
                return (StatsTimer) monitorMap.get(name);
            else {
                final StatsConfig statsConfig = new StatsConfig.Builder().withPercentiles(new double[] { 95, 99 })
                        .withPublishMax(true).withPublishMin(true)
                        .withPublishMean(true).withPublishCount(true).withSampleSize(sampleSize.get()).build();
                final StatsTimer _duration = new StatsTimer(getMonitorConfig(metricName, appName, null, serverGroupName, metric, reason, status), statsConfig, TimeUnit.MILLISECONDS);
                monitorMap.put(name, _duration);
                DefaultMonitorRegistry.getInstance().register(_duration);
                return _duration;
            }
        } finally {
            writeLock.unlock();
        }
    }

    public static String getMetricName(String appName, String cacheName, String metric) {
        return new StringBuilder(128).append(appName).append((cacheName == null ? "-" : new StringBuilder(64).append("-").append(cacheName).append("-").toString())).append(metric).toString();
    }

    public static MonitorConfig getMonitorConfig(String appName, String cacheName, String metric) {
        return getMonitorConfig(getMetricName(appName, cacheName, metric), appName, cacheName, metric);
    }

    public static MonitorConfig getMonitorConfig(String name, String appName, String cacheName, String metric) {
        Builder builder = MonitorConfig.builder(name).withTag("APP", appName).withTag("METRIC", metric).withTag(OWNER);
        if (cacheName != null && cacheName.length() > 0) {
            builder = builder.withTag("CACHE", cacheName);
        }
        return builder.build();
    }

    public static MonitorConfig getMonitorConfig(String name, String appName, String cacheName, String serverGroup, String metric) {
        Builder builder = MonitorConfig.builder(name).withTag("APP", appName).withTag("METRIC", metric).withTag(OWNER);
        if (cacheName != null && cacheName.length() > 0) {
            builder = builder.withTag("CACHE", cacheName);
        }
        if (serverGroup != null && serverGroup.length() > 0) {
            builder = builder.withTag("ServerGroup", serverGroup);
        }
        return builder.build();
    }

    public static MonitorConfig getMonitorConfig(String name, String appName, String cacheName, String serverGroup, String metric, String reason, String status) {
        Builder builder = MonitorConfig.builder(name).withTag("APP", appName).withTag("METRIC", metric).withTag(OWNER);
        if (cacheName != null && cacheName.length() > 0) {
            builder = builder.withTag("CACHE", cacheName);
        }
        if (serverGroup != null && serverGroup.length() > 0) {
            builder = builder.withTag("ServerGroup", serverGroup);
        }
        if (reason != null && reason.length() > 0) {
            builder = builder.withTag("Reason", reason);
        }
        if (status != null && status.length() > 0) {
            builder = builder.withTag("Status", status);
        }
        return builder.build();
    }

    public static Timer getStatsTimer(String name) {
        Timer timer = timerMap.get(name);
        if (timer != null) return timer;
        writeLock.lock();
        try {
            if (timerMap.containsKey(name)) {
                return timerMap.get(name);
            } else {
                final StatsConfig statsConfig = new StatsConfig.Builder().withPercentiles(new double[] { 95, 99 })
                        .withPublishMax(true).withPublishMin(true).withPublishMean(true)
                        .withPublishCount(true).withSampleSize(sampleSize.get()).build();
                final MonitorConfig monitorConfig = MonitorConfig.builder(name).withTag(OWNER).build();
                timer = new StatsTimer(monitorConfig, statsConfig, TimeUnit.MILLISECONDS);
                DefaultMonitorRegistry.getInstance().register(timer);
                timerMap.put(name, timer);
                return timer;
            }
        } finally {
            writeLock.unlock();
        }
    }

    public static DistributionSummary getDistributionSummary(String name, String appName, String serverGroup) {
        final String metricName = getMetricName(appName, serverGroup, name);
        final DistributionSummary _ds = distributionSummaryMap.get(metricName);
        if(_ds != null) return _ds;
        final Registry registry = Spectator.globalRegistry(); //_poolManager.getRegistry();
        if (registry != null) {
            Id id = registry.createId(name);
            id = id.withTag("owner", "evcache");
            id = id.withTag("APP", appName);
            if(serverGroup != null) id = id.withTag("ServerGroup", serverGroup);
            final DistributionSummary ds = registry.distributionSummary(id);
            if (!Monitors.isObjectRegistered(ds)) Monitors.registerObject(ds);
            distributionSummaryMap.put(metricName, ds);
            return ds;
        }
        return null;
    }

}