package it.polimi.middleware.messages;

/**
 * When something goes wrong, the client receives this
 */
public class ReplyErrorMsg extends ServiceMessage {

    private static final long serialVersionUID = 77124090000123199L;

    private final String description;

    public ReplyErrorMsg(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }
}
