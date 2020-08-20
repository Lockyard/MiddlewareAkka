package it.polimi.middleware.messages;

import akka.actor.ActorRef;

import java.io.Serializable;

/**
 * A message from client which updates the value on the server where the  key is the specified one
 */
public class PutMsg extends ServiceMessage implements Serializable {
    /**
     * Useful for Java when serializing class. Better to assign a UID to the class than making it assigned by Java
     */
    private static final long serialVersionUID = 77124090000123303L;

    private static final byte DEFAULT_ALIVE_STEPS = 64;

    private final String key;
    private final String val;

    private long clientID = 0;

    private ActorRef sender;

    //for how many passages of message at most this message will remain alive
    private byte aliveSteps;
    /**
     * Used by the server to recognize order of incoming messages. Needed to keep consistency from client's point of view
     */
    private long newness;


    /**
     * Default PutMsg, which will wait a reply after 1 replica wrote the new value specified
     * @param key the key for the store
     * @param val the new value to be inserted
     */
    public PutMsg(String key, String val) {
        this.key = key;
        this.val = val;
        //default newness is 0
        newness = 0;
        sender = ActorRef.noSender();
        aliveSteps = DEFAULT_ALIVE_STEPS;
    }



    /**
     * Reduce the alive steps of this message
     * @return true if it has finished the alive steps, false if is still alive
     */
    public boolean reduceAliveSteps() {
        aliveSteps--;
        return aliveSteps <= 0;
    }

    public byte getAliveSteps() {
        return aliveSteps;
    }

    //getters, setters

    public final String getKey() {
        return key;
    }

    public final String getVal() {
        return val;
    }

    public void setClientID(long clientID) {
        this.clientID = clientID;
    }

    public final long getClientID() {
        return clientID;
    }

    public void setNewness(long newness) {
        this.newness = newness;
    }

    public long getNewness() {
        return newness;
    }

    public void setSender(ActorRef sender) {
        this.sender = sender;
    }

    public ActorRef sender() {
        return sender;
    }


    @Override
    public String toString() {
        return "PutMsg[Key:" + key + ", Value:" + val + ", Newness:" + newness + "]";
    }
}
