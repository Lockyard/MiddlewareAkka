package it.polimi.middleware.server.messages;

import akka.actor.ActorRef;

import java.util.List;
import java.util.Map;

/**
 * Used to update the status of a StoreNode wrt the "copy hierarchy", meaning the fact that the node could become a
 * leader or update its position in the copy order
 * Stores 2 ActorRefs, related to the next and previous neighbours in a list.
 * Also stores the information of which is the leader replica, used if is requested to copy the data from the leader
 * replica
 */
public class UpdateStoreNodeStatusMsg extends ServerMessage {

    private static final long serialVersionUID = 881240900123206L;

    private final List<List<ActorRef>> nodesOfPartition;

    private final Map<Integer, ActorRef> oldLeaders;

    private final long updateID;


    public UpdateStoreNodeStatusMsg(List<List<ActorRef>> nodesOfPartition, Map<Integer, ActorRef> oldLeaders,
                                    long updateID) {
        this.nodesOfPartition = nodesOfPartition;
        this.oldLeaders = oldLeaders;
        this.updateID = updateID;
    }

    public List<List<ActorRef>> getNodesOfPartition() {
        return nodesOfPartition;
    }

    public Map<Integer, ActorRef> getOldLeaders() {
        return oldLeaders;
    }

    public long getUpdateID() {
        return updateID;
    }
}
