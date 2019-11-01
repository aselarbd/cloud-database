package de.tum.i13.server.kv;

import de.tum.i13.shared.KVItem;

import java.nio.file.Path;

public class LSMLog {

    private int size;

    public LSMLog(Path dir) {

    }

    public void append(KVItem kvItem) {
        size++;
    }

    public int size() {
        return size;
    }

}
