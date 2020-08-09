package it.polimi.middleware.messages;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.List;

/**
 * Reply by the server to a greeting message.
 * Contains the client id so that a client can identify itself and a list of nodes which will be the entry point to
 * the system for that client
 */
public class ReplyGreetingMsg implements Serializable {
    /**
     * Useful for Java when serializing class. Better to assign a UID to the class than making it assigned by Java
     */
    private static final long serialVersionUID = 88124019300123242L;

    private final long clientID;

    private final List<ActorRef> entryNodes;

    public ReplyGreetingMsg(long clientID, List<ActorRef> entryNodes) {
        this.clientID = clientID;
        this.entryNodes = entryNodes;
    }

    public List<ActorRef> getEntryNodes() {
        return entryNodes;
    }

    public long getClientID() {
        return clientID;
    }
}
