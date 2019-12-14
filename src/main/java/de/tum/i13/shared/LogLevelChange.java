package de.tum.i13.shared;

import java.util.logging.Level;

public class LogLevelChange {
    private final Level previousLevel;
    private final Level newLevel;

    public LogLevelChange(Level previousLevel, Level newLevel) {

        this.previousLevel = previousLevel;
        this.newLevel = newLevel;
    }

    public Level getPreviousLevel() {
        return previousLevel;
    }

    public Level getNewLevel() {
        return newLevel;
    }
}
