package it.polimi.middleware.messages;

import java.io.Serializable;

public class ReplyPutMsg extends ServiceMessage implements Serializable {

    private static final long serialVersionUID = 91999929292998L;

    private final boolean success;

    public ReplyPutMsg(boolean success) {
        this.success = success;
    }

    public boolean success() {
        return success;
    }
}
