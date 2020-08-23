package it.polimi.middleware.server.messages;

public class UpdateAllCompleteMsg extends ServerMessage {

    private static final long serialVersionUID = 88124090000123223L;

    private final long updateID;

    private final boolean isAnotherUpdateIncoming;

    public UpdateAllCompleteMsg(long updateID, boolean isAnotherUpdateIncoming) {
        this.updateID = updateID;
        this.isAnotherUpdateIncoming = isAnotherUpdateIncoming;
    }

    public long getUpdateID() {
        return updateID;
    }

    public boolean isAnotherUpdateIncoming() {
        return isAnotherUpdateIncoming;
    }
}
