package it.polimi.middleware.server.messages;

import akka.actor.ActorRef;

/**
 * The message the store manager sends to a node when is up in the cluster
 */
public class GrantAccessToStoreMsg extends ServerMessage {

    private static final long serialVersionUID = 881240900123223L;

    private final ActorRef storeManagerRef;
    private final int nodeNumber;
    private final boolean requestActivation;


    public GrantAccessToStoreMsg(ActorRef storeManagerRef, int nodeNumber, boolean requestActivation) {
        this.nodeNumber = nodeNumber;
        this.storeManagerRef = storeManagerRef;
        this.requestActivation = requestActivation;
    }


    public int getNodeNumber() {
        return nodeNumber;
    }

    public ActorRef getStoreManagerRef() {
        return storeManagerRef;
    }

    public boolean mustRequestActivation() {
        return requestActivation;
    }
}
