package it.polimi.middleware.client;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import it.polimi.middleware.messages.GetMsg;
import it.polimi.middleware.messages.ReplyGetMsg;
import it.polimi.middleware.messages.ReplyPutMsg;

import java.io.File;
import java.util.Scanner;

public class ClientApp {

    public static void main (String[] args) {
        final Config conf = ConfigFactory.parseFile(new File("conf/client.conf"));
        final ActorSystem sys = ActorSystem.create("Client", conf);
        final ActorRef client = sys.actorOf(ClientActor.props("ServerSystem@127.0.0.1:9000"), "ClientActor");

        final Scanner scanner = new Scanner(System.in);

        while (true) {
            final String content = scanner.nextLine();
            if (content.equals("quit")) {
                break;
            } else {
                client.tell(new GetMsg(content), ActorRef.noSender());
            }
        }

        scanner.close();
        sys.terminate();

    }

    public static void receiveGetReply(ReplyGetMsg msg) {
        System.out.println("Client received replyGetMSG: " + msg);
    }

    public static void receivePutReply(ReplyPutMsg msg) {
        System.out.println("Client received replyPutMSG: " + msg);
    }
}
