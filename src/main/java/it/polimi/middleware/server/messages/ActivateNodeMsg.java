package it.polimi.middleware.server.messages;

import akka.actor.ActorRef;

import java.util.List;

/**
 * A message to be sent to a store node when has to activate and go live.
 * If mustRequestData is on true, tell the store node to ask for existing data from other nodes
 * since the system is already live
 */
public class ActivateNodeMsg extends ServerMessage {
    private static final long serialVersionUID = 881240900123200L;

    private final List<List<ActorRef>> nodesOfPartition;

    private final boolean mustRequestData;

    private final long updateID;

    public ActivateNodeMsg(List<List<ActorRef>> nodesOfPartition,boolean mustRequestData, long updateID) {
        this.nodesOfPartition = nodesOfPartition;
        this.mustRequestData = mustRequestData;
        this.updateID = updateID;
    }

    public List<List<ActorRef>> getNodesOfPartition() {
        return nodesOfPartition;
    }

    public boolean mustRequestData() {
        return mustRequestData;
    }

    public long getUpdateID() {
        return updateID;
    }
}
