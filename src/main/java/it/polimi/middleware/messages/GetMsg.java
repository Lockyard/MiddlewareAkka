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
    private static final long serialVersionUID = 771240900123300L;

    private static final int DEFAULT_ALIVE_STEPS = 64;
    //for how many passages of message at most this message will remain alive
    private int aliveSteps;

    private final String key;

    private long clientID = 0;

    private ActorRef sender;

    /**
     * Id of the operation of a user for a given partition (the partition of the key)
     */
    private long clientOpID;

    public GetMsg(String key) {
        this.key = key;
        aliveSteps = DEFAULT_ALIVE_STEPS;
    }

    public final String getKey() {
        return key;
    }

    /**
     * Reduce the alive steps of this message
     * @return true if it has finished the alive steps, false if is still alive
     */
    public boolean reduceAliveSteps() {
        aliveSteps--;
        return aliveSteps <= 0;
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

    public void setClientOpID(long clientOpID) {
        this.clientOpID = clientOpID;
    }

    public long getClientOpID() {
        return clientOpID;
    }

    @Override
    public String toString() {
        return "GetMsg[K:"+key+"]";
    }
}
