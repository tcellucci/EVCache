package com.netflix.evcache.config;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.netflix.archaius.api.Property;
import com.netflix.archaius.api.PropertyFactory;
import com.netflix.archaius.api.PropertyListener;

public class Archaius2PropertyRepo implements PropertyRepo {
    private interface Prop<T> extends Supplier<T> {
        Supplier<T> onChange(Runnable r);
    }
    
    private final PropertyFactory propertyFactory;

    public Archaius2PropertyRepo(PropertyFactory propertyFactory) {
        this.propertyFactory = propertyFactory;
    }

    @Override
    public Supplier<Boolean> getProperty(String propertyKey, Boolean defaultValue) {
        return new Archaius2Prop<Boolean>(propertyFactory.getProperty(propertyKey).asBoolean(defaultValue));
    }

    @Override
    public Supplier<Integer> getProperty(String propertyKey, Integer defaultValue) {
        return new Archaius2Prop<Integer>(propertyFactory.getProperty(propertyKey).asInteger(defaultValue));
    }

    @Override
    public Supplier<Long> getProperty(String propertyKey, Long defaultValue) {
        return new Archaius2Prop<Long>(propertyFactory.getProperty(propertyKey).asLong(defaultValue));
    }

    @Override
    public Supplier<String> getProperty(String propertyKey, String defaultValue) {
        return new Archaius2Prop<String>(propertyFactory.getProperty(propertyKey).asString(defaultValue));
    }

    @Override
    public Supplier<Set<String>> getProperty(String propertyKey, Set<String> defaultValue) {
        return new Archaius2Prop<Set<String>>(propertyFactory.getProperty(propertyKey).asType(setParser, ""));
    }

    @Override
    public Supplier<Boolean> getProperty(String overrideKey, String primaryKey, Boolean defaultValue) {
        return new Archaius2ChainedProp<Boolean>(propertyFactory.getProperty(overrideKey).asBoolean(null),
                propertyFactory.getProperty(primaryKey).asBoolean(defaultValue));
    }

    @Override
    public Supplier<String> getProperty(String overrideKey, String primaryKey, String defaultValue) {
        return new Archaius2ChainedProp<String>(propertyFactory.getProperty(overrideKey).asString(null),
                propertyFactory.getProperty(primaryKey).asString(defaultValue));
    }

    @Override
    public Supplier<Long> getProperty(String overrideKey, String primaryKey, Long defaultValue) {
        return new Archaius2ChainedProp<Long>(propertyFactory.getProperty(overrideKey).asLong(null),
                propertyFactory.getProperty(primaryKey).asLong(defaultValue));
    }

    @Override
    public Supplier<Integer> getProperty(String overrideKey, String primaryKey, Integer defaultValue) {
        return new Archaius2ChainedProp<Integer>(propertyFactory.getProperty(overrideKey).asInteger(null),
                propertyFactory.getProperty(primaryKey).asInteger(defaultValue));
    }

    @Override
    public Supplier<Integer> getProperty(String propertyKey, Supplier<Integer> defaultValueSupplier) {
        return new Archaius2SupplierProp<Integer>(propertyFactory.getProperty(propertyKey).asInteger(null),
                defaultValueSupplier);
    }

    private final Function<String, Set<String>> setParser = value -> {
        final Set<String> rv;
        if (value != null && !value.isEmpty()) {
            Predicate<String> emptyStringFilter = String::isEmpty;
            rv =  Arrays.stream(value.split("\\s*,\\s*")).filter(emptyStringFilter.negate()).collect(Collectors.toSet());
        } else {
            rv = Collections.emptySet();
        }
        return rv;
    };

    private static class RunnablePropertyListener<T> implements PropertyListener<T> {
        private final Runnable callback;

        RunnablePropertyListener(Runnable callback) {
            this.callback = callback;
        }

        @Override
        public void onChange(T value) {
            callback.run();
        }

        @Override
        public void onParseError(Throwable error) {
        }

    }

    private class Archaius2Prop<T> implements Prop<T> {
        private final Property<T> archaiusProperty;

        Archaius2Prop(Property<T> archaiusProperty) {
            this.archaiusProperty = archaiusProperty;
        }

        @Override
        public T get() {
            return archaiusProperty.get();
        }

        @Override
        public Supplier<T> onChange(Runnable callback) {
            archaiusProperty.addListener(new RunnablePropertyListener<>(callback));
            return this;
        }

        @Override
        public String toString() {
            return String.format("Archaius2Prop - value: %s", archaiusProperty.get());
        }
    }

    private class Archaius2ChainedProp<T> implements Prop<T> {
        private final Property<T> overrideProperty;
        private final Property<T> primaryProperty;

        Archaius2ChainedProp(Property<T> overrideProperty, Property<T> primaryProperty) {
            this.overrideProperty = overrideProperty;
            this.primaryProperty = primaryProperty;
        }

        @Override
        public T get() {
            return Optional.ofNullable(overrideProperty.get()).orElse(primaryProperty.get());
        }

        @Override
        public Supplier<T> onChange(Runnable callback) {
            overrideProperty.addListener(new RunnablePropertyListener<>(callback));
            primaryProperty.addListener(new RunnablePropertyListener<>(callback));
            return this;
        }
        
        @Override
        public String toString() {
            return String.format("Archaius2ChainedProp - override: %s, primary: %s", overrideProperty.get(), primaryProperty.get());
        }

    }

    private class Archaius2SupplierProp<T> implements Prop<T> {
        private final Property<T> overrideProperty;
        private final Supplier<T> primarySupplier;

        Archaius2SupplierProp(Property<T> overrideProperty, Supplier<T> primarySupplier) {
            this.overrideProperty = overrideProperty;
            this.primarySupplier = primarySupplier;
        }

        @Override
        public T get() {
            return Optional.ofNullable(overrideProperty.get()).orElse(primarySupplier.get());
        }

        @Override
        public Supplier<T> onChange(Runnable callback) {
            overrideProperty.addListener(new RunnablePropertyListener<>(callback));
            if (primarySupplier instanceof Prop) {
                ((Prop<T>) primarySupplier).onChange(callback);
            }
            return this;
        }

        @Override
        public String toString() {
            return String.format("Archaius2SupplierProp - override: %s, primary: %s", overrideProperty.get(), primarySupplier.get());
        }
    }

    @Override
    public <T> Supplier<T> onChange(Supplier<T> property, Runnable callback) {
        if (property instanceof Prop) {
            ((Prop<T>)property).onChange(callback);
        }
        return null;
    }
}
