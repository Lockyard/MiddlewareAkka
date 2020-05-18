package it.polimi.middleware.messages;

import java.io.Serializable;

/**
 * Reply of a get message
 */
public class ReplyGetMsg extends ServiceMessage implements Serializable {

    private static final long serialVersionUID = 91999929292999L;

    private final String content, key;

    public ReplyGetMsg(String key, String content) {
        this.content = content;
        this.key = key;
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

    @Override
    public String toString() {
        return isNullContent() ? "ReplyGetMsg[null @K:"+key+"]" : "ReplyGetMsg[K:" + key + ", V:" + content + "]";
    }
}
