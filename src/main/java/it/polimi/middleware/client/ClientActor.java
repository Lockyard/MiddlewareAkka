package it.polimi.middleware.client;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import it.polimi.middleware.messages.*;
import it.polimi.middleware.util.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Actor which is used by the client app to communicate with the server.
 * Tells and receive messages to/from it.
 */
public class ClientActor extends AbstractActor {

    private ActorSelection server;
    private String serverAddress;
    private long clientID = 0;
    private boolean isConnected = false;
    /**
     * The only nodes this client can use to communicate to the server
     */
    private List<ActorRef> accessNodes;
    //index used to do round-robin on the accessNodes
    private int i = 0;


    public ClientActor(String serverAddress) {
        this.serverAddress = serverAddress;
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        server = getContext().actorSelection("akka.tcp://" + serverAddress + "/user/MasterNode");
        System.out.println("ClientActor Prestart, server is at path: " + "akka.tcp://" + serverAddress + "/user/MasterNode");
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                //if it receives a get or put message, tell it to the server as the sender
                .match(GetMsg.class, this::onGetMsg)
                .match(PutMsg.class, this::onPutMsg)
                //if it receives a reply, tell it to the client app
                .match(ReplyGetMsg.class, ClientApp::receiveGetReply)
                .match(ReplyPutMsg.class, ClientApp::receivePutReply)
                //greetings and reply
                .match(GreetingMsg.class, msg -> server.tell(msg, self()))
                .match(GreetingReplyMsg.class, this::onGreetingReplyMessage)
                .build();
    }


    private void onGetMsg(GetMsg msg) {
        msg.setClientID(clientID);
        accessNodes.get(i).tell(msg, self());
        roundRobin();
    }

    private void onPutMsg(PutMsg msg) {
        msg.setClientID(clientID);
        accessNodes.get(i).tell(msg, self());
        roundRobin();
    }

    private void roundRobin() {
        i = (i+1) % accessNodes.size();
    }

    private void onGreetingReplyMessage(GreetingReplyMsg msg) {
        Logger.std.dlog("GreetingReply received");
        if(msg.isSuccessful()) {
            isConnected = true;
            clientID = msg.getClientID();
            if(accessNodes == null) {
                accessNodes = new ArrayList<>(msg.getTotalAssignedActors());
            }
            accessNodes.add(msg.getAssignedActor());
            ClientApp.receiveGreetingReplyUpdate(isConnected, "Received store node address (" + msg.getAssignedActor().path().name()+")" +
                    " , " + accessNodes.size() + "/" + msg.getTotalAssignedActors());
        } else {
            ClientApp.receiveGreetingReplyUpdate(isConnected, "Greeting with server failed [" + msg.getDescription()+"]");
        }
    }


    public static Props props(String serverAddress) {
        return Props.create(ClientActor.class, serverAddress);
    }
}
