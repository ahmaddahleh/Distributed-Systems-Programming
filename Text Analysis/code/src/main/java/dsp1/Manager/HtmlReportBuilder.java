package dsp1.Manager;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;

public final class HtmlReportBuilder {

    private HtmlReportBuilder() {
    }

    public static String build(String taskId, String bucketName, List<String[]> entries) {
        StringBuilder html = new StringBuilder();

        html.append("<html><head><title>Task ")
                .append(HtmlEscaper.text(taskId))
                .append(" - Analysis Summary</title>")
                .append("<style>")
                .append("body{font-family:Arial,sans-serif;margin:25px;background:#f9f9f9;}")
                .append("h1{color:#333;border-bottom:2px solid #007bff;padding-bottom:8px;}")
                .append("table{width:100%;border-collapse:collapse;margin-top:20px;}")
                .append("th,td{border:1px solid #ddd;padding:8px;text-align:left;}")
                .append("th{background:#007bff;color:white;}")
                .append(".error{background:#fdd;color:#900;font-weight:bold;}")
                .append("</style></head><body>");

        html.append("<h1>Results for Task: ").append(HtmlEscaper.text(taskId)).append("</h1>");
        html.append("<table>");
        html.append("<tr><th>Subtask</th><th>Analysis Type</th><th>Input File</th><th>Output / Error</th></tr>");

        for (String[] row : entries) {
            String subTaskId = row.length == 4 ? row[0] : "";
            String analysisType = row.length == 4 ? row[1] : row[0];
            String inputUrl = row.length == 4 ? row[2] : row[1];
            String outputField = row.length == 4 ? row[3] : row[2];

            html.append("<tr>");
            html.append("<td>").append(HtmlEscaper.text(subTaskId)).append("</td>");
            html.append("<td>").append(HtmlEscaper.text(analysisType)).append("</td>");
            html.append("<td>");
            appendSafeHttpLinkOrText(html, inputUrl, inputUrl);
            html.append("</td>");

            if (outputField != null && outputField.startsWith("ERROR:")) {
                html.append("<td class='error'>").append(HtmlEscaper.text(outputField)).append("</td>");
            } else {
                String publicLink = buildS3PublicLink(bucketName, outputField);
                html.append("<td>");
                appendSafeHttpLinkOrText(html, publicLink, "View Output");
                html.append("</td>");
            }

            html.append("</tr>");
        }

        html.append("</table></body></html>");
        return html.toString();
    }

    static void appendSafeHttpLinkOrText(StringBuilder html, String href, String text) {
        if (isSafeHttpUrl(href)) {
            html.append("<a href='")
                    .append(HtmlEscaper.attribute(href))
                    .append("' target='_blank'>")
                    .append(HtmlEscaper.text(text))
                    .append("</a>");
        } else {
            html.append(HtmlEscaper.text(text));
        }
    }

    static boolean isSafeHttpUrl(String value) {
        try {
            URI uri = URI.create(value);
            String scheme = uri.getScheme();
            return scheme != null
                    && ("http".equals(scheme.toLowerCase(Locale.ROOT))
                            || "https".equals(scheme.toLowerCase(Locale.ROOT)));
        } catch (Exception e) {
            return false;
        }
    }

    private static String buildS3PublicLink(String bucketName, String key) {
        String encodedKey = URLEncoder.encode(key == null ? "" : key, StandardCharsets.UTF_8);
        return "https://" + bucketName + ".s3.us-east-1.amazonaws.com/" + encodedKey;
    }
}
