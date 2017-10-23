package com.netflix.evcache.config;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Supplier;

import com.netflix.config.ChainedDynamicProperty;
import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicLongProperty;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;
import com.netflix.config.DynamicStringSetProperty;
import com.netflix.config.Property;
import com.netflix.config.PropertyWrapper;
import com.netflix.config.ChainedDynamicProperty.ChainLink;

/**
 * Archaius 1 implementation of PropertyRepo with com.netflix.config.** 
 * 
 */
public class Archaius1PropertyRepo implements PropertyRepo {
	static class DynamicPropertyAdapter<T> implements Prop<T> {
		private Property<T> property;

		DynamicPropertyAdapter(Property<T> property) {
			this.property = property;
		}

		@Override
		public T get() {
			if  (property instanceof ChainLink) {
				return ((ChainLink<T>)property).get();
			}
			else {
				return property.getValue();
			}
		}

		@Override
		public Prop<T> onChange(Runnable r) {
			property.addCallback(r);
			return this;
		}
	}
	
	static class ChainedPropertyAdapter<T> implements Prop<T> {
		private ChainLink<T> chainLink;
		private Property<T> property;

		ChainedPropertyAdapter(ChainLink<T> chainLink, Property<T> property) {
			this.chainLink = chainLink;
			this.property = property;
		}

		@Override
		public T get() {
			return chainLink.get();
		}

		@Override
		public Prop<T> onChange(Runnable r) {
			chainLink.addCallback(r);
			property.addCallback(r);
			return this;
		}
	}
	

	// archaius 1 fast property factory
	private final DynamicPropertyFactory propertyFactory;

	// cache of Prop instances indexed by property name
	private final ConcurrentMap<String, Prop> propertyLookup;

	public Archaius1PropertyRepo() {
		this(DynamicPropertyFactory.getInstance());
	}

	public Archaius1PropertyRepo(DynamicPropertyFactory propertyFactory) {
		this.propertyFactory = propertyFactory;
		this.propertyLookup = new ConcurrentHashMap<>();
	}

	private <T> Prop<T> getProperty(String propertyKey, Function<String, Property> propertyFactory) {
		return propertyLookup.computeIfAbsent(propertyKey, k -> {
			return new DynamicPropertyAdapter(propertyFactory.apply(k));
		});
	}
	
	private <T> Prop<T> getChainedProperty(String propertyKey, Function<String, Prop<T>> propertyFactory) {
		return propertyLookup.computeIfAbsent(propertyKey, k -> {
			return propertyFactory.apply(k);
		});
	}	

	/* (non-Javadoc)
	 * @see com.netflix.evcache.config.PropRepository#getProperty(java.lang.String, java.lang.String, java.lang.Boolean)
	 */
	@Override
	public Prop<Boolean> getProperty(String overrideKey, String primaryKey, Boolean defaultValue) {
		Function<String, Prop<Boolean>> propFactory = k -> {
			ChainedDynamicProperty.DynamicBooleanPropertyThatSupportsNull baseProperty = new ChainedDynamicProperty.DynamicBooleanPropertyThatSupportsNull(
					primaryKey, defaultValue);
			ChainLink<Boolean> overrideProperty = new ChainedDynamicProperty.BooleanProperty(overrideKey, baseProperty);
			return new ChainedPropertyAdapter(overrideProperty, baseProperty);
		};
		return getChainedProperty(overrideKey + primaryKey, propFactory);
	}

	/* (non-Javadoc)
	 * @see com.netflix.evcache.config.PropRepository#getProperty(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public Prop<String> getProperty(String overrideKey, String primaryKey, String defaultValue) {
		Function<String, Prop<String>> propFactory = k -> {
			DynamicStringProperty baseProp = new DynamicStringProperty(primaryKey, defaultValue);
			ChainLink<String> overrideProperty = new ChainedDynamicProperty.StringProperty(overrideKey, baseProp);
			return new ChainedPropertyAdapter(overrideProperty, baseProp);
		};
		return getChainedProperty(overrideKey + primaryKey, propFactory);
	}

	/* (non-Javadoc)
	 * @see com.netflix.evcache.config.PropRepository#getProperty(java.lang.String, java.lang.String, java.lang.Long)
	 */
	@Override
	public Prop<Long> getProperty(String overrideKey, String primaryKey, Long defaultValue) {
		Function<String, Prop<Long>> propFactory = k -> {
			DynamicLongProperty baseProp = new DynamicLongProperty(primaryKey, defaultValue);
			ChainLink<Long> overrideProperty = new ChainedDynamicProperty.LongProperty(overrideKey, baseProp);
			return new ChainedPropertyAdapter(overrideProperty, baseProp);
		};
		return getChainedProperty(overrideKey + primaryKey, propFactory);
	}

	/* (non-Javadoc)
	 * @see com.netflix.evcache.config.PropRepository#getProperty(java.lang.String, java.lang.String, java.lang.Integer)
	 */
	@Override
	public Prop<Integer> getProperty(String overrideKey, String primaryKey, Integer defaultValue) {
		Function<String, Prop<Integer>> propFactory = k -> {
			DynamicIntProperty baseProp = new DynamicIntProperty(primaryKey, defaultValue);
			ChainLink<Integer> overrideProperty = new ChainedDynamicProperty.IntProperty(overrideKey, baseProp);
			return new ChainedPropertyAdapter(overrideProperty, baseProp);
		};
		return getChainedProperty(overrideKey + primaryKey, propFactory);
	}
	
	@Override
	public Prop<Integer> getProperty(String key, Supplier<Integer> defaultValueSupplier) {
		Function<String, Property> propFactory = k -> {
			return new PropertyWrapper<Integer>(key, null) {
				@Override
				public Integer getValue() {
					return Optional.of(getDynamicProperty().getInteger()).orElse(defaultValueSupplier.get());
				}
				
			};
		};
		return getProperty(key, propFactory);
	}

	

	/* (non-Javadoc)
	 * @see com.netflix.evcache.config.PropRepository#getProperty(java.lang.String, java.lang.Boolean)
	 */
	@Override
	public Prop<Boolean> getProperty(String propertyKey, Boolean defaultValue) {
		return getProperty(propertyKey, key -> propertyFactory.getBooleanProperty(key, defaultValue));

	}

	/* (non-Javadoc)
	 * @see com.netflix.evcache.config.PropRepository#getProperty(java.lang.String, java.lang.Integer)
	 */
	@Override
	public Prop<Integer> getProperty(String propertyKey, Integer defaultValue) {
		return getProperty(propertyKey, key -> propertyFactory.getIntProperty(key, defaultValue));
	}

	/* (non-Javadoc)
	 * @see com.netflix.evcache.config.PropRepository#getProperty(java.lang.String, java.lang.Long)
	 */
	@Override
	public Prop<Long> getProperty(String propertyKey, Long defaultValue) {
		return getProperty(propertyKey, key -> propertyFactory.getLongProperty(key, defaultValue));
	}

	/* (non-Javadoc)
	 * @see com.netflix.evcache.config.PropRepository#getProperty(java.lang.String, java.lang.String)
	 */
	@Override
	public Prop<String> getProperty(String propertyKey, String defaultValue) {
		return getProperty(propertyKey, key -> propertyFactory.getStringProperty(key, defaultValue));
	}

	/* (non-Javadoc)
	 * @see com.netflix.evcache.config.PropRepository#getProperty(java.lang.String, java.util.Set)
	 */
	@Override
	public Prop<Set<String>> getProperty(String propertyKey, Set<String> defaultValue) {
		return getProperty(propertyKey, key -> new DynamicStringSetProperty(key, ""));
	}

}