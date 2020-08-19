package it.polimi.middleware.messages;

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

    /**
     * Used by the server to recognize order of incoming messages. Needed to keep consistency from client's point of view
     */
    private long newness=0;

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

    public void setNewness(long newness) {
        this.newness = newness;
    }

    public long getNewness() {
        return newness;
    }

    @Override
    public String toString() {
        return "GetMsg[K:"+key+", Newness:"+newness+"]";
    }
}
