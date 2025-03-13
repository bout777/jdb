import com.idme.table.Page;
import com.idme.table.Record;
import org.junit.Test;

public class PageTest {
    @Test
    public void testInsertRecord(){
        Page page = new Page();
        Record record = new Record();
        record.value = new int[]{1,2,3};
        record.size = 12;
        page.insertRecord(record);
    }
}
