package it.polimi.middleware.server.messages;

import akka.actor.ActorRef;

/**
 * A message which carries the information about a client which will be assigned to a node
 */
public class ClientAssignMsg extends ServerMessage {

    private static final long serialVersionUID = 88124090000123201L;

    private final long clientID;
    private final int nodesAssigned;
    private final ActorRef clientRef;

    public ClientAssignMsg(long clientID, int nodesAssigned, ActorRef clientRef) {
        this.clientID = clientID;
        this.nodesAssigned = nodesAssigned;
        this.clientRef = clientRef;
    }

    public long getClientID() {
        return clientID;
    }


    public int getNodesAssigned() {
        return nodesAssigned;
    }

    public ActorRef getClientRef() {
        return clientRef;
    }
}
