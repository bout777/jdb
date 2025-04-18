import com.jdb.common.Value;
import com.jdb.table.RowData;

import java.util.Random;

public class TestUtil {
    private static Random r = new Random();
    public static RowData generateRecord(int i) {
        RowData rowData = new RowData();

        rowData.primaryKey = i;
        rowData.isDeleted = 0;

        rowData.size += Integer.BYTES * 2 + Byte.BYTES;

        rowData.values.add(Value.ofString("hehe"));
        rowData.values.add(Value.ofInt(r.nextInt()));

        for (Value val : rowData.values) {
            rowData.size += val.getBytes();
        }

        return rowData;
    }
}
