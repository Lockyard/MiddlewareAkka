package it.polimi.middleware.messages;

import akka.actor.ActorRef;

import java.io.Serializable;

/**
 * Get Message, sent from client to get from the server the value stored where the key is the specified one.
 */
public class GetMsg extends ServiceMessage implements Serializable {

    /**
     * Useful for Java when serializing class. Better to assign a UID to the class than making it assigned by Java
     */
    private static final long serialVersionUID = 77124090000123300L;

    private final String key;

    private long clientID = 0;

    private ActorRef sender;

    public GetMsg(String key) {
        this.key = key;
    }

    public final String getKey() {
        return key;
    }

    public void setClientID(long clientID) {
        this.clientID = clientID;
    }

    public final long getClientID() {
        return clientID;
    }

    public void setSender(ActorRef sender) {
        this.sender = sender;
    }

    public ActorRef sender() {
        return sender;
    }

    @Override
    public String toString() {
        return "GetMsg[K:"+key+"]";
    }
}
