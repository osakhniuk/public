package serialization;

import java.io.IOException;
import java.io.FileInputStream;


// Data time column.
public class DateTimeColumn extends Column<Double> {
    private static final String TYPE = Types.DATE_TIME;
    private static final double _doubleNone = -62135607600000000.0;

    public String getType() {
        return TYPE;
    }

    public DateTimeColumn(String path) throws IOException {
        super(path);
    }

    public DateTimeColumn(String path, Double[] values) throws IOException {
        this(path);
        addAll(values);
    }

    public void encode(BufferAccessor buf) throws IOException {
        close();
        buf.writeInt32(3); // Encoder ID
        buf.writeFloat64ListFromStream(new FileInputStream(path), length);
    }

    public void add(Double value) throws IOException {
        long bits = Double.doubleToLongBits((value != null) ? value : _doubleNone);
        stream.writeByte((byte)bits);
        stream.writeByte((byte)(bits >> 8));
        stream.writeByte((byte)(bits >> 16));
        stream.writeByte((byte)(bits >> 24));
        stream.writeByte((byte)(bits >> 32));
        stream.writeByte((byte)(bits >> 40));
        stream.writeByte((byte)(bits >> 48));
        stream.writeByte((byte)(bits >> 56));
        length++;
    }

    public void addAll(Double[] values) throws IOException {
        for (Double value : values)
            add(value);
    }

    @Override
    public long memoryInBytes() {
        return length * 8;
    }

    /*
    // Note: Following code is saved only for information purposes
    //import java.time.*;
    private void rawEncoder(BufferAccessor buf) throws IOException {
        // Convert to separate vectors
        short[] year = new short[length];
        byte[] month = new byte[length];
        byte[] day = new byte[length];
        byte[] hour = null;
        byte[] minute = null;
        byte[] second = null;
        short[] millisecond = null;

        for (int n = 0; n < length; n++) {
            if (data[n] != _doubleNone) {
                LocalDateTime dateTime = Instant.ofEpochMilli((long)data[n] / 1000).atZone(ZoneId.of("UTC+0")).toLocalDateTime();
                year[n] = (short) dateTime.getYear();
                month[n] = (byte) dateTime.getMonthValue();
                day[n] = (byte) dateTime.getDayOfMonth();
                hour = setDataValueByteArray(hour, n, (byte) dateTime.getHour());
                minute = setDataValueByteArray(minute, n, (byte) dateTime.getMinute());
                second = setDataValueByteArray(second, n, (byte) dateTime.getSecond());
                millisecond = setDataValueShortArray(millisecond, n, (short) (dateTime.getNano() / 1000000));
            } else {
                year[n] = 1;
                month[n] = 1;
                day[n] = 1;
            }
        }

        buf.writeInt32(1); // Encoder ID
        writeShortArray(buf, year);
        writeByteArray(buf, month);
        writeByteArray(buf, day);
        writeByteArray(buf, hour);
        writeByteArray(buf, minute);
        writeByteArray(buf, second);
        writeShortArray(buf, millisecond);
        writeShortArray(buf, null);
    }

    private short[] setDataValueShortArray(short[] array, int idx, short value) {
        if (value != 0) {
            if (array == null)
                array = new short[length];
            array[idx] = value;
        }
        return array;
    }

    private byte[] setDataValueByteArray(byte[] array, int idx, byte value) {
        if (value != 0) {
            if (array == null)
                array = new byte[length];
            array[idx] = value;
        }
        return array;
    }

    private static void writeShortArray(BufferAccessor buf, short[] array) throws IOException {
        buf.writeInt8((byte)((array != null) ? 1 : 0));
        buf.writeInt8((byte)0); // Archive
        if (array != null)
            buf.writeInt16List(array);
    }

    private static void writeByteArray(BufferAccessor buf, byte[] array) throws IOException {
        buf.writeInt8((byte)((array != null) ? 1 : 0));
        buf.writeInt8((byte)0); // Archive
        if (array != null)
            buf.writeInt8List(array);
    }
    */
}
