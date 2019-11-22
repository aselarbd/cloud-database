package de.tum.i13.shared;

/**
 * Generic object factory to obtain new instances and make testing easier.
 * As it only has one method, it can be conveniently used as functional interface with lambda expressions.
 *
 * @param <T> The type of which to create a new instance.
 */
public interface Factory<T>
{
    T getInstance();
}
