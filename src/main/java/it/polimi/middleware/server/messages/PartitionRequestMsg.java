package it.polimi.middleware.server.messages;

public class PartitionRequestMsg  extends ServerMessage {

    private static final long serialVersionUID = 88124090000123220L;

    private final int partitionRequired;

    public PartitionRequestMsg(int partitionRequired) {
        this.partitionRequired = partitionRequired;
    }

    public int getPartitionRequired() {
        return partitionRequired;
    }
}
