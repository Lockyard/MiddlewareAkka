package it.polimi.middleware.server.messages;

/**
 * Message sent in reply to a check of data consistency for a specific partition
 */
public class DataConsistencyOkMsg extends ServerMessage{
    private static final long serialVersionUID = 881240900123228L;

    private final int partition;

    private final long updateID;

    public DataConsistencyOkMsg(int partition, long updateID) {
        this.updateID = updateID;
        this.partition = partition;
    }

    public int getPartition() {
        return partition;
    }

    public long getUpdateID() {
        return updateID;
    }
}
