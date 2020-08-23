package it.polimi.middleware.server.messages;

/**
 * A message for replicas to request from another replica a copy of some data which is valid
 */
public class DataRequestMsg extends ServerMessage {

    private static final long serialVersionUID = 881240900123208L;

    private final String key;

    public DataRequestMsg(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }
}