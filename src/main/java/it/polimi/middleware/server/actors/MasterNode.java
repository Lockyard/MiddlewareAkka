package it.polimi.middleware.server.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.polimi.middleware.messages.ServiceMessage;

public class MasterNode extends AbstractActor {
    private ActorRef storeManager;


    @Override
    public void preStart() throws Exception {
        super.preStart();
        storeManager = getContext().actorOf(StoreManager.props());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                //forward service messages to the storeManager
                .match(ServiceMessage.class, msg -> storeManager.tell(msg, self()))
                .build();
    }

    public static Props props() {
        return Props.create(MasterNode.class);
    }
}
