package dsp1;

import java.util.Base64;
import java.nio.charset.StandardCharsets;

public final class UserDataEncoder {

    private UserDataEncoder() {
    }

    public static String encode(String script) {
        return Base64.getEncoder().encodeToString(script.getBytes(StandardCharsets.UTF_8));
    }
}
