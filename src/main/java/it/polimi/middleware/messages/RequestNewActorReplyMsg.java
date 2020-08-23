package it.polimi.middleware.messages;

import akka.actor.ActorRef;

public class RequestNewActorReplyMsg extends ServiceMessage {

    private static final long serialVersionUID = 771240900123308L;

    private final ActorRef assignedActor;

    public RequestNewActorReplyMsg(ActorRef assignedActor) {
        this.assignedActor = assignedActor;
    }

    public ActorRef getAssignedActor() {
        return assignedActor;
    }
}
