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
    private long newness;

    /**
     * How many replicas must write the new value before sending a reply to this message
     */
    private byte writesUntilReply;


    /**
     * Default PutMsg, which will wait a reply after 1 replica wrote the new value specified
     * @param key the key for the store
     * @param val the new value to be inserted
     */
    public PutMsg(String key, String val) {
        this.key = key;
        this.val = val;
        writesUntilReply = 1;
    }

    /**
     * Create a PutMsg specifying also how many replicas should write the new value before responding.
     * @param key the key for the store
     * @param val the new value to be inserted
     * @param writesUntilReply <= 0 to request a reply after every replicas' write. >0 to specify the number of replicas
     */
    public PutMsg(String key, String val, byte writesUntilReply) {
        this.key = key;
        this.val = val;
        this.writesUntilReply = writesUntilReply;
    }



    @Override
    public String toString() {
        return "PutMsg[Key:" + key + ", Value:" + val + ", Newness: " + newness + "]";
    }

    /**
     * Ask to this message if there should be a reply now. This method is supposed to be called after a write on the
     * current replica managing this message occurred.
     * @return true if the replica managing this message should reply now, false if should forward to the next replica
     */
    public boolean shouldReplyAfterThisWrite() {
        //if 0 or less this means that was requested that all replicas must write, so the message itself will never
        //specify when it's the moment to write
        if(writesUntilReply <= 0) {
            return false;
            //else reduce the number of writes. If now is 0, return true to tell that is time to reply
        } else {
            writesUntilReply--;
            return writesUntilReply == 0;
        }
    }

    public final String getKey() {
        return key;
    }

    public final String getVal() {
        return val;
    }

    public void setNewness(long newness) {
        this.newness = newness;
    }

    public long getNewness() {
        return newness;
    }

    public byte getWritesUntilReply() {
        return writesUntilReply;
    }

    public void setWritesUntilReply(byte writesUntilReply) {
        this.writesUntilReply = writesUntilReply;
    }
}
