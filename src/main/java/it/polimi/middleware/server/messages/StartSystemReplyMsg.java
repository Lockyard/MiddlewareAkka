package it.polimi.middleware.server.messages;

public class StartSystemReplyMsg extends ServerMessage {

    private static final long serialVersionUID = 88124090000123205L;

    private final boolean success;
    private String description;

    public StartSystemReplyMsg(boolean success) {
        this.success = success;
        description = "";
    }

    public StartSystemReplyMsg(boolean success, String description) {
        this.success = success;
        this.description = description;
    }

    public boolean isSuccessful() {
        return success;
    }

    public String getDescription() {
        return description;
    }
}
