package it.polimi.middleware.client;

import akka.actor.AbstractActor;
import akka.actor.ActorSelection;
import akka.actor.Props;
import it.polimi.middleware.messages.GetMsg;
import it.polimi.middleware.messages.PutMsg;
import it.polimi.middleware.messages.ReplyGetMsg;
import it.polimi.middleware.messages.ReplyPutMsg;

/**
 * Actor which is used by the client app to communicate with the server.
 * Tells and receive messages to/from it.
 */
public class ClientActor extends AbstractActor {

    private ActorSelection server;
    private String serverAddress;
    private ClientApp clientApp;

    @SuppressWarnings("unused")
    public ClientActor(String serverAddress, ClientApp clientApp) {
        this.serverAddress = serverAddress;
        this.clientApp = clientApp;
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        server = getContext().actorSelection("akka.tcp://" + serverAddress + " /user/MasterNode");
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                //if it receives a get or put message, tell it to the server as the sender
                .match(GetMsg.class, msg -> server.tell(msg, self()))
                .match(PutMsg.class, msg -> server.tell(msg, self()))
                //if it receives a reply, tell it to the client app
                .match(ReplyGetMsg.class, msg -> clientApp.receiveGetReply(msg))
                .match(ReplyPutMsg.class, msg -> clientApp.receivePutReply(msg))
                .build();
    }


    public static Props props(String serverAddress, ClientApp clientApp) {
        return Props.create(ClientActor.class, serverAddress, clientApp);
    }
}