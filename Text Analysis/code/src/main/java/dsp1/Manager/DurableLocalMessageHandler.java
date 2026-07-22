package dsp1.Manager;

import org.json.JSONObject;
import software.amazon.awssdk.services.sqs.model.Message;

public final class DurableLocalMessageHandler {

    private DurableLocalMessageHandler() {
    }

    @FunctionalInterface
    public interface JobPersister {
        void persist(JSONObject request);
    }

    @FunctionalInterface
    public interface MessageDeleter {
        void delete(Message message);
    }

    public static boolean persistThenAcknowledge(Message message, JobPersister persister, MessageDeleter deleter) {
        JSONObject obj = new JSONObject(message.body());
        persister.persist(obj);
        deleter.delete(message);
        return true;
    }
}
