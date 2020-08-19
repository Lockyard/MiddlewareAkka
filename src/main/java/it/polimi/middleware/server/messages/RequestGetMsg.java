package it.polimi.middleware.server.messages;

public class RequestGetMsg extends ServerMessage {
    private static final long serialVersionUID = 88124090000123203L;

    private final String key;

    public RequestGetMsg(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }
}
