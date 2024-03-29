package it.polimi.middleware.server.messages;

public class StartSystemMsg extends ServerMessage {

    private static final long serialVersionUID = 881240900123204L;

    private final boolean forceNodeCreation;

    public StartSystemMsg(boolean forceNodeCreation) {
        this.forceNodeCreation = forceNodeCreation;
    }

    public boolean forceNodeCreation() {
        return forceNodeCreation;
    }
}
