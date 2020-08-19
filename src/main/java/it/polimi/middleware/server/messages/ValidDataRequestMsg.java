package it.polimi.middleware.server.messages;

/**
 * A message for replicas to request from another replica a copy of some data which is valid
 */
public class ValidDataRequestMsg extends ServerMessage {

    private static final long serialVersionUID = 88124090000123208L;

    private final String key;

    public ValidDataRequestMsg(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }
}
