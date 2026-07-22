package dsp1.Manager;

public interface StorageGateway {
    String readObjectAsString(String bucket, String key);

    void putHtml(String bucket, String key, String html);
}
