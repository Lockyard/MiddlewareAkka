package it.polimi.middleware.server.messages;

import it.polimi.middleware.server.store.ValueData;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class PartitionRequestReplyMsg extends ServerMessage {

    private static final long serialVersionUID = 881240900123221L;

    private final int partitionRequired;

    private final HashMap<String, ValueData> partitionData;

    private final Map<Long, Map<String, Long>> clientToOpIDMap;

    private final long updateID;

    public PartitionRequestReplyMsg(int partitionRequired, HashMap<String, ValueData> partitionData,
                                    Map<Long, Map<String, Long>> clientToOpIDMap, long updateID) {
        this.partitionRequired = partitionRequired;
        this.partitionData = partitionData;
        this.clientToOpIDMap = clientToOpIDMap;
        this.updateID = updateID;
    }

    public int getPartitionRequired() {
        return partitionRequired;
    }

    public HashMap<String, ValueData> getPartitionData() {
        return partitionData;
    }

    public Map<Long, Map<String, Long>> getClientToOpIDMap() {
        return clientToOpIDMap;
    }

    public long getUpdateID() {
        return updateID;
    }
}
