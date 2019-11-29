package de.tum.i13.kvtp2;

@FunctionalInterface
public interface Factory<T> {
    T getInstance();
}
