package dsp1.Manager;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HtmlReportBuilderTest {

    @Test
    void escapesUntrustedTextAndAttributes() {
        String html = HtmlReportBuilder.build(
                "job-<script>\"'&",
                "bucket-name",
                List.<String[]>of(new String[] {
                        "job-1:0\"><script>",
                        "POS & <tag>",
                        "https://example.com/a?x=\"<&y='",
                        "ERROR: failed \" '<>& <script>"
                }));

        assertTrue(html.contains("job-&lt;script&gt;&quot;&#x27;&amp;"));
        assertTrue(html.contains("job-1:0&quot;&gt;&lt;script&gt;"));
        assertTrue(html.contains("POS &amp; &lt;tag&gt;"));
        assertTrue(html.contains("ERROR: failed &quot; &#x27;&lt;&gt;&amp; &lt;script&gt;"));
        assertFalse(html.contains("<script>"));
        assertFalse(html.contains("failed \" '<>&"));
    }

    @Test
    void permitsOnlyHttpAndHttpsInputLinks() {
        String html = HtmlReportBuilder.build(
                "job-1",
                "bucket-name",
                List.of(
                        new String[] { "job-1:0", "POS", "javascript:alert(1)", "result/key" },
                        new String[] { "job-1:1", "POS", "not a url", "result/key2" },
                        new String[] { "job-1:2", "POS", "https://example.com/doc.txt", "result/key3" }));

        assertFalse(html.contains("href='javascript:alert(1)'"));
        assertTrue(html.contains("javascript:alert(1)"));
        assertTrue(html.contains("not a url"));
        assertTrue(html.contains("href='https://example.com/doc.txt'"));
    }

    @Test
    void malformedUrlsRenderAsEscapedPlainText() {
        String html = HtmlReportBuilder.build(
                "job-1",
                "bucket-name",
                List.<String[]>of(new String[] {
                        "job-1:0",
                        "DEPENDENCY",
                        "https://example .com/<bad>",
                        "ERROR: malformed & bad"
                }));

        assertFalse(html.contains("href='https://example .com/<bad>'"));
        assertTrue(html.contains("https://example .com/&lt;bad&gt;"));
        assertTrue(html.contains("ERROR: malformed &amp; bad"));
    }
}
