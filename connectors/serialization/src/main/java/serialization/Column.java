package serialization;

import java.io.*;
import java.util.*;


// Column.
public abstract class Column<T> {
    private static final int DEFAULT_BUFFER_SIZE = 1048576;

    DataOutputStream stream;
    String path;
    public String name;
    public int length = 0;
    public Map<String, String> tags = new HashMap<>();

    public Column() {}

    public Column(String path) throws IOException {
        this.path = path;
        stream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path), DEFAULT_BUFFER_SIZE));
    }

    public void close() throws IOException {
        stream.close();
    }

    public abstract String getType();
    public abstract void encode(BufferAccessor buf) throws IOException;
    public abstract void add(T value) throws IOException;
    public abstract void addAll(T[] value) throws IOException;
    public abstract long memoryInBytes();
}
