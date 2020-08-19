package it.polimi.middleware.messages;

import java.io.Serializable;
import java.util.StringTokenizer;

public class ReplyPutMsg extends ServiceMessage implements Serializable {

    private static final long serialVersionUID = 77124090000123306L;

    private final boolean success;
    private final String key, content;

    public ReplyPutMsg(String key, String content, boolean success) {
        this.key = key;
        this.content = content;
        this.success = success;
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
        return "ReplyPutMsg[K:"+key+", V:" + content + ", " + (success ? "OK]" : "FAILED]");
    }
}
