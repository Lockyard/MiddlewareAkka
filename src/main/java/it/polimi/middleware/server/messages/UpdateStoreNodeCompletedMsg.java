package it.polimi.middleware.server.messages;

/**
 * The message sent from a storenode to the store manager in reply to an update
 */
public class UpdateStoreNodeCompletedMsg extends ServerMessage {

    private static final long serialVersionUID = 88124090000123222L;

    private final long updateID;

    public UpdateStoreNodeCompletedMsg(long updateID) {
        this.updateID = updateID;
    }

    public long getUpdateID() {
        return updateID;
    }
}
