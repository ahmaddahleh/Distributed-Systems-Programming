package dsp1.Worker;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class ResultKeyBuilderTest {

    @Test
    void generatedS3KeysAreDeterministicAndDistinctBySubTask() {
        String first = ResultKeyBuilder.build("job-1", "job-1:0", "https://example.com/a.txt", "POS");
        String firstAgain = ResultKeyBuilder.build("job-1", "job-1:0", "https://example.com/a.txt", "POS");
        String second = ResultKeyBuilder.build("job-1", "job-1:1", "https://example.com/a.txt", "POS");

        assertEquals(first, firstAgain);
        assertNotEquals(first, second);
    }

    @Test
    void localFileNamesAreDeterministicAndDistinctBySubTask() {
        assertEquals("job-1_0_inputfile", ResultKeyBuilder.localInputFileName("job-1:0"));
        assertEquals("job-1_1_output.txt", ResultKeyBuilder.localOutputFileName("job-1:1"));
        assertNotEquals(
                ResultKeyBuilder.localInputFileName("job-1:0"),
                ResultKeyBuilder.localInputFileName("job-1:1"));
    }
}
