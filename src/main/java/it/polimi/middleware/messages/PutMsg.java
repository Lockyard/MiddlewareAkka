package it.polimi.middleware.messages;

import java.io.Serializable;

/**
 * A message from client which updates the value on the server where the  key is the specified one
 */
public class PutMsg extends ServiceMessage implements Serializable {
    /**
     * Useful for Java when serializing class. Better to assign a UID to the class than making it assigned by Java
     */
    private static final long serialVersionUID = 5982003500003L;

    private final String key;
    private final String val;
    /**
     * Used by the server to recognize order of incoming messages. Needed to keep consistency from client's point of view
     */
    private long msgID;



    public PutMsg(String key, String val) {
        this.key = key;
        this.val = val;
    }


    public final String getKey() {
        return key;
    }

    public final String getVal() {
        return val;
    }

    public void setMsgID(long msgID) {
        this.msgID = msgID;
    }

    public long getMsgID() {
        return msgID;
    }

    @Override
    public String toString() {
        return "PutMsg[K:" + key + ", V:" + val + "]";
    }
}
