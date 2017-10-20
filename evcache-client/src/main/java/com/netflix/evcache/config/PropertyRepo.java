package com.netflix.evcache.config;

import java.util.Set;
import java.util.function.Supplier;

public interface PropertyRepo {
	public interface Prop<T> extends Supplier<T> {
		Prop<T> onChange(Runnable r);
	}

	Prop<Boolean> getProperty(String propertyKey, Boolean defaultValue);

	Prop<Integer> getProperty(String propertyKey, Integer defaultValue);

	Prop<Long> getProperty(String propertyKey, Long defaultValue);

	Prop<String> getProperty(String propertyKey, String defaultValue);

	Prop<Set<String>> getProperty(String propertyKey, Set<String> defaultValue);

	Prop<Boolean> getProperty(String overrideKey, String primaryKey, Boolean defaultValue);

	Prop<String> getProperty(String overrideKey, String primaryKey, String defaultValue);

	Prop<Long> getProperty(String overrideKey, String primaryKey, Long defaultValue);

	Prop<Integer> getProperty(String overrideKey, String primaryKey, Integer defaultValue);

	Prop<Integer> getProperty(String propertyKey, Supplier<Integer> defaultValue);
	
}