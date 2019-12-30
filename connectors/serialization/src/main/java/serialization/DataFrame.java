package serialization;

import java.util.*;
import java.io.IOException;


// Data frame.
public class DataFrame {
    public String name;
    public Integer rowCount;
    public List<Column> columns = new ArrayList<>();
    public Map<String, String> tags;

    public DataFrame() {
    }

    public void addColumn(Column col) {
        rowCount = col.length;
        columns.add(col);
    }

    public void addColumns(List<Column> cols) {
        if (cols.size() > 0) {
            rowCount = cols.get(0).length;
            columns.addAll(cols);
        }
    }

    public byte[] toByteArray(String path) throws IOException {
        DataFrame[] tables = {this};
        return (new TablesBlob(path, tables)).toByteArray();
    }
}
