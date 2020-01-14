package serialization;

import java.io.IOException;
import java.io.FileInputStream;


// Bool column.
public class BoolColumn extends Column<Boolean> {
    private static final String TYPE = Types.BOOL;

    private int data;

    public String getType() {
        return TYPE;
    }

    public BoolColumn(String path) throws IOException {
        super(path);
    }

    public BoolColumn(String path, Boolean[] values) throws IOException {
        this(path);
        addAll(values);
    }

    public void encode(BufferAccessor buf) throws IOException {
        if (length % 0x20 != 0)
            flush();
        close();
        buf.writeInt32(1);  // Encoder ID
        buf.writeInt64(length);
        buf.writeInt8((byte)0);
        buf.writeUint32ListFromStream(new FileInputStream(path), ((length + 0x1F) / 0x20));
    }

    public void add(Boolean value) throws IOException {
        if ((value != null) && value)
            data |= 1 << ((length % 0x20) & 0x1F);
        if (++length % 0x20 == 0)
            flush();
    }

    public void addAll(Boolean[] values) throws IOException {
        for (Boolean value : values)
            add(value);
    }

    @Override
    public long memoryInBytes() {
        return (length + 0x1F) / 8;
    }

    private void flush() throws IOException {
        stream.writeByte((byte)data);
        stream.writeByte((byte)(data >> 8));
        stream.writeByte((byte)(data >> 16));
        stream.writeByte((byte)(data >> 24));
        data = 0;
    }
}
