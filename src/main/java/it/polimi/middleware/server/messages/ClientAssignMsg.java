package it.polimi.middleware.server.messages;

import akka.actor.ActorRef;

/**
 * A message which carries the information about a client which will be assigned to a node
 */
public class ClientAssignMsg extends ServerMessage {

    private static final long serialVersionUID = 881240900123201L;

    private final long clientID;
    private final int nodesAssigned;
    private final boolean isSingleAssignment;
    private final ActorRef clientRef;

    /**
     * This constructor is used when the greeting is made: assign a client and add the information
     * of how many nodes in total will be assigned
     * @param clientID the id of the client
     * @param nodesAssigned the number of nodes assigned
     */
    public ClientAssignMsg(long clientID, int nodesAssigned, ActorRef clientRef) {
        this.clientID = clientID;
        this.nodesAssigned = nodesAssigned;
        isSingleAssignment = false;
        this.clientRef = clientRef;
    }


    /**
     * This constructor is used when a single assignment is made: the client has already some nodes assigned
     * @param clientID the id of the client
     */
    public ClientAssignMsg(long clientID, ActorRef clientRef) {
        this.clientID = clientID;
        nodesAssigned = 1;
        isSingleAssignment = true;
        this.clientRef = clientRef;
    }

    public long getClientID() {
        return clientID;
    }


    public int getNodesAssigned() {
        return nodesAssigned;
    }

    public boolean isSingleAssignment() {
        return isSingleAssignment;
    }

    public ActorRef getClientRef() {
        return clientRef;
    }
}
