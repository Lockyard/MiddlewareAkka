package it.polimi.middleware.server.messages;

public class ValidDataReplyMsg extends ServerMessage {
    String content;

    public ValidDataReplyMsg(String content) {
        this.content = content;
    }

    public String getContent() {
        return content;
    }
}
