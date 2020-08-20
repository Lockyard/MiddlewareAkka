package it.polimi.middleware.server.messages;

import akka.actor.ActorRef;

public class RequestActivateMsg extends ServerMessage {
    private final ActorRef storeNodeRef;

    public RequestActivateMsg(ActorRef storeNodeRef) {
        this.storeNodeRef = storeNodeRef;
    }

    public ActorRef getStoreNodeRef() {
        return storeNodeRef;
    }
}
