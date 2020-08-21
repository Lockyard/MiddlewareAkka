package it.polimi.middleware.server.messages;

import akka.actor.ActorRef;

import java.util.List;

/**
 * Used to update the status of a StoreNode wrt the "copy hierarchy", meaning the fact that the node could become a
 * leader or update its position in the copy order
 * Stores 2 ActorRefs, related to the next and previous neighbours in a list.
 * Also stores the information of which is the leader replica, used if is requested to copy the data from the leader
 * replica
 */
public class UpdateStoreNodeStatusMsg extends ServerMessage {

    private static final long serialVersionUID = 88124090000123206L;

    private final List<List<ActorRef>> nodesOfPartition;


    public UpdateStoreNodeStatusMsg(List<List<ActorRef>> nodesOfPartition) {
        this.nodesOfPartition = nodesOfPartition;
    }

    public List<List<ActorRef>> getNodesOfPartition() {
        return nodesOfPartition;
    }
}
