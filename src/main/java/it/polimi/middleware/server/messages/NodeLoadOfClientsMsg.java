package it.polimi.middleware.server.messages;

/**
 * Sent by a node to notify the server of its load of clients. A node can send it when it wants
 */
public class NodeLoadOfClientsMsg extends ServerMessage {

    private static final long serialVersionUID = 881240900123225L;

    private final int clientLoad;

    public NodeLoadOfClientsMsg(int clientLoad) {
        this.clientLoad = clientLoad;
    }

    public int getClientLoad() {
        return clientLoad;
    }
}
