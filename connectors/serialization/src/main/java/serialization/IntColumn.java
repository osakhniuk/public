package serialization;

import java.io.IOException;
import java.io.FileInputStream;


// Integer column.
public class IntColumn extends Column<Integer> {
    private static final String TYPE = Types.INT;
    private static final int None = -2147483648;

    public String getType() {
        return TYPE;
    }

    public IntColumn(String path) throws IOException  {
        super(path);
    }

    public IntColumn(String path, Integer[] values) throws IOException  {
        this(path);
        addAll(values);
    }

    public void encode(BufferAccessor buf) throws IOException {
        close();
        buf.writeInt32(1);  // Encoder ID
        buf.writeInt8((byte)0);   // Archive
        buf.writeInt32ListFromStream(new FileInputStream(path), length);
    }

    public void add(Integer value) throws IOException {
        int _value = (value != null) ? value : None;
        stream.writeByte((byte)_value);
        stream.writeByte((byte)(_value >> 8));
        stream.writeByte((byte)(_value >> 16));
        stream.writeByte((byte)(_value >> 24));
        length++;
    }

    public void addAll(Integer[] values) throws IOException {
        for (Integer value : values)
            add(value);
    }

    @Override
    public long memoryInBytes() {
        return length * 4;
    }
}
