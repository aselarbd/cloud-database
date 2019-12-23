package de.tum.i13.shared;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Convenience wrapper around util.Logger functionality
 */
public class Log {
    Logger logger;

    public Log(Class<?> scope) {
        logger = Logger.getLogger(scope.getName());
    }

    public void fine(String msg) {
        logger.fine(msg);
    }

    public void info(String msg) {
        logger.info(msg);
    }

    public void info(String msg, Throwable e) {
        logger.log(Level.INFO, msg, e);
    }

    public void warning(String msg) {
        logger.warning(msg);
    }

    public void warning(String msg, Throwable e) {
        logger.log(Level.WARNING, msg, e);
    }

    public void severe(String msg, Throwable e) {
        logger.log(Level.SEVERE, msg, e);
    }
}
