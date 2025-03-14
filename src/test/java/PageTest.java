import com.idme.storage.Disk;
import com.idme.table.Page;
import com.idme.table.Record;
import org.junit.Test;

import java.util.Arrays;

public class PageTest {
    @Test
    public void testInsertRecord(){
        Page page = new Page(0);
        Record record = new Record();
        record.value = new int[]{1,2,3};
        record.size = 12;
        page.insertRecord(record);

        Disk  disk= new Disk();
        disk.writePage("test.db",0,page.getData());
        byte[] data = page.getData();
        Arrays.fill(data, (byte)0);
        disk.readPage("test.db",0,page.getData());
    }
}
