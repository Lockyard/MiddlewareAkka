package it.polimi.middleware.server.messages;

import akka.actor.ActorRef;

public class RequestActivateMsg extends ServerMessage {

    private static final long serialVersionUID = 881240900123224L;

    private ActorRef checkSMRef;
    public RequestActivateMsg(ActorRef checkSMRef) {
        this.checkSMRef = checkSMRef;
    }

    public ActorRef getCheckSMRef() {
        return checkSMRef;
    }
}
