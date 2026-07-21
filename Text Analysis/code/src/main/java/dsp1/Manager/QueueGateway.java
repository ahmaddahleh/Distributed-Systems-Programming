package dsp1.Manager;

import org.json.JSONObject;

public interface QueueGateway {
    void sendWorkerTask(JSONObject task);

    void sendLocalCompletion(JSONObject completion);
}
