package it.polimi.middleware.server.messages;

import akka.actor.ActorRef;

/**
 * The message the store manager sends to a node when is up in the cluster
 */
public class GrantAccessToStoreMsg extends ServerMessage {
    private final ActorRef storeManagerRef;
    private final int nodeNumber;

    public GrantAccessToStoreMsg(ActorRef storeManagerRef, int nodeNumber) {
        this.nodeNumber = nodeNumber;
        this.storeManagerRef = storeManagerRef;
    }

    public int getNodeNumber() {
        return nodeNumber;
    }

    public ActorRef getStoreManagerRef() {
        return storeManagerRef;
    }
}
