package it.polimi.middleware.server.actors;

import akka.actor.AbstractActor;
import akka.actor.Props;

public class StoreManager extends AbstractActor {
    @Override
    public Receive createReceive() {
        return null;
    }

    public static Props props() {
        return Props.create(StoreManager.class);
    }
}
