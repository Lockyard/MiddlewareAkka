package it.polimi.middleware.server.messages;

public class DataReplyMsg extends ServerMessage {

    private static final long serialVersionUID = 881240900123207L;

    String content;

    public DataReplyMsg(String content) {
        this.content = content;
    }

    public String getContent() {
        return content;
    }
}
