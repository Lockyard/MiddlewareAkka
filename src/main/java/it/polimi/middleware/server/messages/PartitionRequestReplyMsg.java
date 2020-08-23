package it.polimi.middleware.server.messages;

import it.polimi.middleware.server.store.ValueData;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class PartitionRequestReplyMsg extends ServerMessage {

    private static final long serialVersionUID = 881240900123221L;

    private final int partitionRequired;

    private final HashMap<String, ValueData> partitionData;

    public PartitionRequestReplyMsg(int partitionRequired, HashMap<String, ValueData> partitionData) {
        this.partitionRequired = partitionRequired;
        this.partitionData = partitionData;
    }

    public int getPartitionRequired() {
        return partitionRequired;
    }

    public HashMap<String, ValueData> getPartitionData() {
        return partitionData;
    }
}
