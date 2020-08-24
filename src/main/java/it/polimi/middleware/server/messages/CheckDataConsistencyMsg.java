package it.polimi.middleware.server.messages;

public class CheckDataConsistencyMsg extends ServerMessage {

    private static final long serialVersionUID = 881240900123227L;


    private final int partition;
    private final long updateValue;

    private final long updateID;

    public CheckDataConsistencyMsg(int partition, long updateValue, long updateID) {
        this.partition = partition;
        this.updateValue = updateValue;
        this.updateID = updateID;
    }

    public int getPartition() {
        return partition;
    }

    public long getUpdateValue() {
        return updateValue;
    }

    public long getUpdateID() {
        return updateID;
    }
}
