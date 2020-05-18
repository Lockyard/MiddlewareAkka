package it.polimi.middleware.server.messages;

public class ValidDataReplyMsg extends ServerMessage {

    private static final long serialVersionUID = 88124090000123248L;

    String content;

    public ValidDataReplyMsg(String content) {
        this.content = content;
    }

    public String getContent() {
        return content;
    }
}
