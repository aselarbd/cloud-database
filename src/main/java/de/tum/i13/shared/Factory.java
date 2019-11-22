package de.tum.i13.shared;

/**
 * Generic object factory to obtain new instances and make testing easier
 *
 * @param <T> The type of which to create a new instance.
 */
public interface Factory<T>
{
    T getInstance();
}
