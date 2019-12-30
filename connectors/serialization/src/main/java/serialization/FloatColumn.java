package serialization;

import java.io.IOException;
import java.io.FileInputStream;


// Float column.
public class FloatColumn extends Column<Float> {
    private static final String TYPE = Types.FLOAT;
    static final double None = 2.6789344063684636e-34;

    public String getType() {
        return TYPE;
    }

    public FloatColumn(String path) throws IOException {
        super(path);
    }

    public FloatColumn(String path, Float[] values) throws IOException {
        this(path);
        addAll(values);
    }

    public void encode(BufferAccessor buf) throws IOException {
        close();
        buf.writeInt32(1);  // Encoder ID
        buf.writeInt8((byte)0);   // Archive
        buf.writeFloat32Stream(new FileInputStream(path), length);
    }

    public void add(Float value) throws IOException {
        stream.writeFloat((value != null) ? value : (float)None);
        length++;
    }

    public void addAll(Float[] values) throws IOException {
        for (Float value : values)
            stream.writeFloat((value != null) ? value : (float)None);
        length += values.length;
    }

    @Override
    public long memoryInBytes() {
        return length * 4;
    }
}
