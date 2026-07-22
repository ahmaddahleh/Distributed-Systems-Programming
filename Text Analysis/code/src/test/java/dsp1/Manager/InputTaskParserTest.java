package dsp1.Manager;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class InputTaskParserTest {

    @Test
    void identicalInputLinesProduceDistinctSubTaskIds() {
        String input = """
                POS\thttps://example.com/a.txt
                POS\thttps://example.com/a.txt
                """;

        List<WorkerTask> tasks = InputTaskParser.parse(input, "job-1");

        assertEquals(2, tasks.size());
        assertEquals("job-1:0", tasks.get(0).subTaskId());
        assertEquals("job-1:1", tasks.get(1).subTaskId());
        assertNotEquals(tasks.get(0).subTaskId(), tasks.get(1).subTaskId());
    }

    @Test
    void malformedLinesAreSkippedAndValidIndexingIsDense() {
        String input = """
                this-line-is-malformed
                POS\thttps://example.com/a.txt

                missing-url-only\t
                DEPENDENCY\thttps://example.com/b.txt
                """;

        List<WorkerTask> tasks = InputTaskParser.parse(input, "job-2");

        assertEquals(2, tasks.size());
        assertEquals("job-2:0", tasks.get(0).subTaskId());
        assertEquals("POS", tasks.get(0).analysis());
        assertEquals("job-2:1", tasks.get(1).subTaskId());
        assertEquals("DEPENDENCY", tasks.get(1).analysis());
    }

    @Test
    void jsonContainsSubTaskId() {
        WorkerTask task = new WorkerTask("job-3", "job-3:7", "POS", "https://example.com/doc.txt");

        assertEquals("job-3:7", task.toJson().getString("subTaskId"));
    }
}
