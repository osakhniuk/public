package serialization;

import java.io.IOException;


// Big integer column.
public class BigIntColumn extends StringColumn {
    static final String TYPE = Types.BIG_INT;

    public String getType() {
        return TYPE;
    }

    public BigIntColumn(String path) throws IOException {
        super(path);
    }

    public BigIntColumn(String path, String[] values) throws IOException {
        super(path, values);
    }

    public void encode(BufferAccessor buf) throws IOException {
        buf.writeInt32(1);
        super.encode(buf);
    }
}
