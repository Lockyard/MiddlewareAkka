package it.polimi.middleware.messages;

import java.io.Serializable;
import java.util.StringTokenizer;

public class ReplyPutMsg extends ServiceMessage implements Serializable {

    private static final long serialVersionUID = 771240900123306L;

    private final boolean success;
    private final String key, content;

    private final long clientOpID;

    public ReplyPutMsg(String key, String content, long clientOpID, boolean success) {
        this.key = key;
        this.content = content;
        this.success = success;
        this.clientOpID = clientOpID;
    }

    public boolean success() {
        return success;
    }

    public String getKey() {
        return key;
    }

    public String getContent() {
        return content;
    }

    public String toString() {
        return "ReplyPutMsg[K:"+key+", V:" + content + ", opid: " +clientOpID + (success ? ", OK]" : ", FAILED]");
    }
}
