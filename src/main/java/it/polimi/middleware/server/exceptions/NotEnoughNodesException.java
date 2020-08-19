package it.polimi.middleware.server.exceptions;

public class NotEnoughNodesException extends Exception {

    private int nodesRequired, nodesProvided;
    private String additionalInfo;

    public NotEnoughNodesException(int nodesRequired, int nodesProvided) {
        this.nodesProvided = nodesProvided;
        this.nodesRequired = nodesRequired;
        additionalInfo = "";
    }

    public NotEnoughNodesException(int nodesRequired, int nodesProvided, String additionalInfo) {
        this.nodesProvided = nodesProvided;
        this.nodesRequired = nodesRequired;
        this.additionalInfo = additionalInfo;
    }

    @Override
    public String toString() {
        return nodesRequired + " nodes are required but " + nodesProvided + " were provided.\n"+additionalInfo;
    }


}
