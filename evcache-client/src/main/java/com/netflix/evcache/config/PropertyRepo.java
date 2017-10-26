package com.netflix.evcache.config;

import java.util.Set;
import java.util.function.Supplier;

public interface PropertyRepo {
    
    Supplier<Boolean> getProperty(String propertyKey, Boolean defaultValue);

    Supplier<Integer> getProperty(String propertyKey, Integer defaultValue);

    Supplier<Long> getProperty(String propertyKey, Long defaultValue);

    Supplier<String> getProperty(String propertyKey, String defaultValue);

    Supplier<Set<String>> getProperty(String propertyKey, Set<String> defaultValue);

    Supplier<Boolean> getProperty(String overrideKey, String primaryKey, Boolean defaultValue);

    Supplier<String> getProperty(String overrideKey, String primaryKey, String defaultValue);

    Supplier<Long> getProperty(String overrideKey, String primaryKey, Long defaultValue);

    Supplier<Integer> getProperty(String overrideKey, String primaryKey, Integer defaultValue);

    Supplier<Integer> getProperty(String propertyKey, Supplier<Integer> defaultValue);

    <T> Supplier<T> onChange(Supplier<T> property, Runnable callback);

}