package it.polimi.middleware.messages;

import akka.actor.ActorRef;

import java.util.ArrayList;

public class RequestNewActorMsg extends ServiceMessage {

    private static final long serialVersionUID = 771240900123307L;

    private final ArrayList<ActorRef> actorsAlreadyAssigned;

    private final long clientID;

    public RequestNewActorMsg(long clientID, ArrayList<ActorRef> actorsAlreadyAssigned) {
        this.clientID = clientID;
        this.actorsAlreadyAssigned = actorsAlreadyAssigned;
    }

    public long getClientID() {
        return clientID;
    }

    public ArrayList<ActorRef> getActorsAlreadyAssigned() {
        return actorsAlreadyAssigned;
    }
}
