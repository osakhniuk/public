package serialization;

import java.io.*;
import java.util.*;
import java.lang.Math;
import java.nio.charset.*;
import java.nio.file.*;
import org.apache.commons.io.IOUtils;


// A convenient class for binary data serialization.
public class BufferAccessor {
    private static final int DEFAULT_BUFFER_SIZE = 1048576;

    // Internal data storage.
    DataOutputStream stream;
    String path;

    // Current position in the buffer.
    public int bufPos = 0;

    // Version of format.
    static final String VERSION = "0.1.0-21a23d8e";

    private static final int TYPE = 0xA000;
    private static final int LIST = 0x0100;

    private static final int STRING = 1;
    private static final int FLOAT_32 = 2;
    private static final int UINT_8 = 3;
    private static final int UINT_16 = 4;
    private static final int UINT_32 = 5;
    private static final int INT_8 = 6;
    private static final int INT_16 = 7;
    private static final int INT_32 = 8;
    //private static final int BOOL = 9;
    private static final int FLOAT_64 = 10;

    private static final int TYPE_COLUMN = TYPE + 40;
    private static final int TYPE_DATA_FRAME = TYPE + 41;
    private static final int TYPE_STRING_MAP = TYPE + 42;

    private static final int TYPE_FLOAT_32_LIST = TYPE + LIST + FLOAT_32;
    private static final int TYPE_FLOAT_64_LIST = TYPE + LIST + FLOAT_64;
    private static final int TYPE_UINT_8_LIST = TYPE + LIST + UINT_8;
    private static final int TYPE_UINT_16_LIST = TYPE + LIST + UINT_16;
    private static final int TYPE_UINT_32_LIST = TYPE + LIST + UINT_32;
    private static final int TYPE_INT_8_LIST = TYPE + LIST + INT_8;
    private static final int TYPE_INT_16_LIST = TYPE + LIST + INT_16;
    private static final int TYPE_INT_32_LIST = TYPE + LIST + INT_32;
    private static final int TYPE_STRING_LIST = TYPE + LIST + STRING;
    //private static final int TYPE_COLUMN_LIST = LIST | TYPE_COLUMN;
    //private static final int TYPE_DATA_FRAME_LIST = LIST | TYPE_DATA_FRAME;

    // Writes two bytes that determine the type of the entity that follows.
    private void writeTypeCode(int typeCode) throws IOException {
        writeInt16((short) typeCode);
    }

    public BufferAccessor(String path) throws IOException {
        this.path = path;
        stream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path), DEFAULT_BUFFER_SIZE));
    }

//    public BufferAccessor(String byte[] list) throws IOException {
//        buf = list;
//        view = new ByteData(buf);
//    }

    void writeString(String value) throws IOException {
        byte[] bytes = value == null ? null : value.getBytes(Charset.forName("UTF-8"));
        writeUint8List(bytes);
    }

    void writeInt8(byte value) throws IOException {
        stream.writeByte(value);
        bufPos++;
    }

    void writeInt16(short value) throws IOException {
        stream.writeByte((byte)value);
        stream.writeByte((byte)(value >> 8));
        bufPos += 2;
    }

    void writeInt32(int value) throws IOException {
        stream.writeByte((byte)value);
        stream.writeByte((byte)(value >> 8));
        stream.writeByte((byte)(value >> 16));
        stream.writeByte((byte)(value >> 24));
        bufPos += 4;
    }

    void writeInt64(long value) throws IOException {
        writeFloat64((double)value);
    }

    void writeFloat32(float value) throws IOException {
        int bits = Float.floatToIntBits(value);
        stream.writeByte((byte)bits);
        stream.writeByte((byte)(bits >> 8));
        stream.writeByte((byte)(bits >> 16));
        stream.writeByte((byte)(bits >> 24));
        bufPos += 4;
    }

    void writeFloat64(double value) throws IOException {
        long bits = Double.doubleToLongBits(value);
        stream.writeByte((byte)bits);
        stream.writeByte((byte)(bits >> 8));
        stream.writeByte((byte)(bits >> 16));
        stream.writeByte((byte)(bits >> 24));
        stream.writeByte((byte)(bits >> 32));
        stream.writeByte((byte)(bits >> 40));
        stream.writeByte((byte)(bits >> 48));
        stream.writeByte((byte)(bits >> 56));
        bufPos += 8;
    }

    void writeFloat32List(float[] values, int... idxs) throws IOException {
        int start = (idxs.length > 0) ? idxs[0] : 0;
        int count = (idxs.length > 1) ? idxs[1] : values.length - start;

        writeTypeCode(TYPE_FLOAT_32_LIST);

        writeInt64(count);
        for (int i = 0; i < count; i++)
            writeFloat32(values[start + i]);
    }

    void writeFloat32ListAsStream(FileInputStream float32stream, int count) throws IOException {
        writeTypeCode(TYPE_FLOAT_32_LIST);
        writeInt64(count);
        IOUtils.copy(float32stream, stream);
        bufPos += count * 4;
    }

    void writeFloat64List(double[] values, int... idxs) throws IOException {
        int start = (idxs.length > 0) ? idxs[0] : 0;
        int count = (idxs.length > 1) ? idxs[1] : values.length - start;

        writeTypeCode(TYPE_FLOAT_64_LIST);

        writeInt64(count);
        for (int i = 0; i < count; i++)
            stream.writeDouble(values[start + i]);
        bufPos += count * 8;
    }

    private void writeBytesList(byte[] values, int... idxs) throws IOException {
        if (values == null) {
            writeInt64(-1);
            return;
        }

        int start = (idxs.length > 0) ? idxs[0] : 0;
        int count = (idxs.length > 1) ? idxs[1] : values.length - start;

        writeInt64(count);
        for (int i = 0; i < count; i++)
            stream.writeByte(values[start + i]);
        bufPos += count;
    }

    void writeUint8List(byte[] values, int... idxs) throws IOException  {
        writeTypeCode(TYPE_UINT_8_LIST);
        writeBytesList(values, idxs);
    }

    void writeInt8List(byte[] values, int... idxs) throws IOException {
        writeTypeCode(TYPE_INT_8_LIST);
        writeBytesList(values, idxs);
    }

    private void writeShortList(short[] values, int... idxs) throws IOException {
        int start = (idxs.length > 0) ? idxs[0] : 0;
        int count = (idxs.length > 1) ? idxs[1] : values.length - start;

        writeInt64(count);
        for (int i = 0; i < count; i++)
            stream.writeShort(values[start + i]);
        bufPos += count * 2;
    }

    void writeInt16List(short[] values, int... idxs) throws IOException {
        writeTypeCode(TYPE_INT_16_LIST);
        writeShortList(values, idxs);
    }

    void writeUint16List(short[] values, int... idxs) throws IOException {
        writeTypeCode(TYPE_UINT_16_LIST);
        writeShortList(values, idxs);
    }

    private void writeIntList(int[] values, int... idxs) throws IOException {
        int start = (idxs.length > 0) ? idxs[0] : 0;
        int count = (idxs.length > 1) ? idxs[1] : values.length - start;

        writeInt64(count);
        for (int i = 0; i < count; i++)
            this.writeInt32(values[start + i]);
    }

    void writeInt32List(int[] values, int... idxs) throws IOException {
        writeTypeCode(TYPE_INT_32_LIST);
        writeIntList(values, idxs);
    }

    void writeUint32List(int[] values, int... idxs) throws IOException {
        writeTypeCode(TYPE_UINT_32_LIST);
        writeIntList(values, idxs);
    }

    void writeIntListAsStream(FileInputStream int32stream, int count) throws IOException {
        writeInt64(count);
        IOUtils.copy(int32stream, stream);
        bufPos += count * 4;
    }

    void writeInt32ListAsStream(FileInputStream int32stream, int count) throws IOException {
        writeTypeCode(TYPE_INT_32_LIST);
        writeIntListAsStream(int32stream, count);
    }

    void writeUint32ListAsStream(FileInputStream int32stream, int count) throws IOException {
        writeTypeCode(TYPE_UINT_32_LIST);
        writeIntListAsStream(int32stream, count);
    }

    void writeStringList(String[] values, int... idxs) throws IOException {
        writeTypeCode(TYPE_STRING_LIST);
        int start = (idxs.length > 0) ? idxs[0] : 0;
        int count = (idxs.length > 1) ? idxs[1] : values.length - start;
        writeTypeCode(TYPE_UINT_8_LIST);
        int begin = bufPos;
        int[] lengths = new int[count];
        writeInt64(0);
        for (int n = 0; n < count; n++) {
            String str = values[start + n];
            if (str != null) {
                lengths[n] = str.length();
                byte[] bytes = str.getBytes(Charset.forName("UTF-8"));
                for (int m = 0; m < bytes.length; m++)
                    stream.writeByte(bytes[m]);
                bufPos += bytes.length;
            } else
                lengths[n] = -1;
        }
        // TODO Issue for Stream! Fix it
        int end = bufPos;
        bufPos = begin;
        writeInt64(end - begin - 8);
        bufPos = end;
        writeInt32List(lengths);
    }

    void writeColumn(Column col) throws IOException {
        writeTypeCode(TYPE_COLUMN);
        writeString(col.name);
        writeString(col.getType());
        writeStringMap(col.tags);
        col.encode(this);
    }

    // Serializes a [DataFrame]. It can be null. Returns columns offsets [offsets].
    int[] writeDataFrame(DataFrame dataFrame) throws IOException {
        writeTypeCode(TYPE_DATA_FRAME);
        writeInt64((dataFrame.rowCount == null) ? -1 : dataFrame.rowCount);
        writeInt64(dataFrame.columns.size());

        writeString(dataFrame.name);
        writeStringMap(dataFrame.tags);
        int[] offsets = new int[dataFrame.columns.size()];
        for (int n = 0; n < dataFrame.columns.size(); n++) {
            offsets[n] = bufPos;
            writeColumn(dataFrame.columns.get(n));
        }

        return offsets;
    }

    void writeStringMap(Map<String, String> map) throws IOException {
        writeTypeCode(TYPE_STRING_MAP);

        if (map == null) {
            writeInt32(-1);
            return;
        }

        writeInt32(map.size());
        for (String key : map.keySet()) {
            writeString(key);
            writeString(map.get(key));
        }
    }

    public byte[] toUint8List() throws IOException {
        stream.close();
        return IOUtils.toByteArray(new FileInputStream(path));
    }

    // Inserts space into buffer on position [pos] with size [size].
    private void _insert(int pos, int size) {
        // TODO Deprecate
        /*
        _ensureSpace(size);
        for (int i = bufPos - 1; i >= pos; i--)
            buf[i + size] = buf[i];
        bufPos += size;
        */
    }

    // Get size of buffer required to write [Uint8List] list to it.
    static int sizeUint8List(byte[] value) {
        // Type code + Length + Data
        return 2 + 8 + ((value == null) ? 0 : value.length);
    }

    // Get size of buffer required to write String to it.
    static int sizeString(String value) {
        byte[] bytes = value == null ? null : value.getBytes(Charset.forName("UTF-8"));
        return sizeUint8List(bytes);
    }

    // Gets buffer as byte array with header information [header].
    public void insertStringHeader(String header) throws IOException {
        _insert(0, sizeString(header));
        int _bufPos = bufPos;
        bufPos = 0;
        writeString(header);
        bufPos = _bufPos;
    }

    public void close() throws IOException {
        stream.close();
    }
}
