package it.polimi.middleware.server.exceptions;

public class NotEnoughNodesException extends Exception {
    private int nodesRequired, nodesProvided;
    public NotEnoughNodesException(int nodesRequired, int nodesProvided) {
        this.nodesProvided = nodesProvided;
        this.nodesRequired = nodesRequired;
    }

    @Override
    public String toString() {
        return nodesRequired + " nodes are required but " + nodesProvided + " were provided.";

    }
}
