package com.netflix.evcache.pool;

import java.lang.management.ManagementFactory;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.evcache.config.CacheConfig;
import com.netflix.evcache.config.CacheConfig.ExecutorConfig;
import com.netflix.evcache.metrics.EVCacheMetricsFactory;
import com.netflix.servo.DefaultMonitorRegistry;
import com.netflix.servo.MonitorRegistry;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.monitor.LongGauge;
import com.netflix.servo.monitor.Monitor;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.monitor.MonitorConfig.Builder;

public class EVCacheExecutor extends ThreadPoolExecutor implements EVCacheExecutorMBean {

    private static final Logger log = LoggerFactory.getLogger(EVCacheExecutor.class);
    private final Supplier<Integer> maxAsyncPoolSize;
    private final Supplier<Integer> coreAsyncPoolSize;
    private final String name;

    public EVCacheExecutor(CacheConfig cacheConfig, int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, RejectedExecutionHandler handler, String name) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit,
                new LinkedBlockingQueue<Runnable>(), 
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat( "EVCacheExecutor-" + name + "-%d").build());
        this.name = name;

        ExecutorConfig executorConfig = cacheConfig.getExecutorConfig(name);
        this.maxAsyncPoolSize = executorConfig.getMaxSize(maximumPoolSize);
        setMaximumPoolSize(maxAsyncPoolSize.get());
        this.coreAsyncPoolSize =  executorConfig.getCoreSize(corePoolSize);
        setCorePoolSize(coreAsyncPoolSize.get());
        setKeepAliveTime(keepAliveTime, unit);
        executorConfig.addCallback(maxAsyncPoolSize, ()->this.setMaximumPoolSize(maxAsyncPoolSize.get()));
        executorConfig.addCallback(coreAsyncPoolSize, ()->{
            setCorePoolSize(coreAsyncPoolSize.get());
            prestartAllCoreThreads();
        });
        
        setupMonitoring(name);
    }

    private void setupMonitoring(String name) {
        try {
            ObjectName mBeanName = ObjectName.getInstance("com.netflix.evcache:Group=ThreadPool,SubGroup="+name);
            MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
            if (mbeanServer.isRegistered(mBeanName)) {
                if (log.isDebugEnabled()) log.debug("MBEAN with name " + mBeanName + " has been registered. Will unregister the previous instance and register a new one.");
                mbeanServer.unregisterMBean(mBeanName);
            }
            mbeanServer.registerMBean(this, mBeanName);
        } catch (Exception e) {
            if (log.isDebugEnabled()) log.debug("Exception", e);
        }
        
        final MonitorRegistry registry = DefaultMonitorRegistry.getInstance();

        registry.register(new Monitor<Number>() {
            final MonitorConfig config;

            {
                config = MonitorConfig.builder("EVCacheExecutor.completedTaskCount").withTag(DataSourceType.COUNTER).withTag(EVCacheMetricsFactory.OWNER).build();
            }

            @Override
            public Number getValue() {
                return Long.valueOf(getCompletedTaskCount());
            }

            @Override
            public Number getValue(int pollerIndex) {
                return getValue();
            }

            @Override
            public MonitorConfig getConfig() {
                return config;
            }
        });
        
        final Builder builder = MonitorConfig.builder("EVCacheExecutor.currentQueueSize").withTag(DataSourceType.GAUGE).withTag(EVCacheMetricsFactory.OWNER);
        final LongGauge queueSize  = new LongGauge(builder.build()) {
            @Override
            public Number getValue() {
                return Long.valueOf(getQueueSize());
            }

            @Override
            public Number getValue(int pollerIndex) {
                return getValue();
            }
        };
        if (registry.isRegistered(queueSize)) registry.unregister(queueSize);
        registry.register(queueSize);
    }

    public void shutdown() {
        try {
            ObjectName mBeanName = ObjectName.getInstance("com.netflix.evcache:Group=ThreadPool,SubGroup="+name);
            MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
            mbeanServer.unregisterMBean(mBeanName);
        } catch (Exception e) {
            if (log.isDebugEnabled()) log.debug("Exception", e);
        }
        super.shutdown();
    }

    @Override
    public int getQueueSize() {
        return getQueue().size();
    }


}
