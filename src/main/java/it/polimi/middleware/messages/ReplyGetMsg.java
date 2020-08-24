package it.polimi.middleware.messages;

import java.io.Serializable;

/**
 * Reply of a get message
 */
public class ReplyGetMsg extends ServiceMessage implements Serializable {

    private static final long serialVersionUID = 771240900123304L;

    private final String content, key;

    private final long clientOpID;

    public ReplyGetMsg(String key, String content, long clientOpID) {
        this.content = content;
        this.key = key;
        this.clientOpID = clientOpID;
    }

    public final String getContent() {
        return content;
    }

    public String getKey() {
        return key;
    }

    public final boolean isNullContent() {
        return content == null;
    }

    public long getClientOpID() {
        return clientOpID;
    }

    @Override
    public String toString() {
        return isNullContent() ? "ReplyGetMsg[null @K:"+key+", opid:"+clientOpID+"]" :
                "ReplyGetMsg[K:" + key + ", V:" + content +", opid:" +clientOpID + "]";
    }
}
