package it.polimi.middleware.server.messages;

public class PartitionRequestMsg  extends ServerMessage {

    private static final long serialVersionUID = 881240900123220L;

    private final int partitionRequired;

    private final long updateID;

    public PartitionRequestMsg(int partitionRequired, long updateID) {
        this.partitionRequired = partitionRequired;
        this.updateID = updateID;
    }

    public int getPartitionRequired() {
        return partitionRequired;
    }

    public long getUpdateID() {
        return updateID;
    }
}
