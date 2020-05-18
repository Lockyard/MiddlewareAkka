package it.polimi.middleware.client;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import it.polimi.middleware.messages.GetMsg;
import it.polimi.middleware.messages.ReplyGetMsg;
import it.polimi.middleware.messages.ReplyPutMsg;
import it.polimi.middleware.messages.ServiceMessage;

import java.io.File;
import java.util.Collections;
import java.util.Scanner;

public class ClientApp {

    public static void main (String[] args) {

        if(args.length > 0 && args [0].equalsIgnoreCase("--help"))
            System.out.println("args: [serverAddress serverPort].\nIf not provided, default values will be used" +
                    "Use 1 argument to insert the address, or use 'd' as 1st arg to set default address but specify port");

        //set server address and port from args. If not provided, default ones are used
        final String serverAddress = (args.length > 0 && !args[0].equals("d")) ? args[0] : "127.0.0.1";
        final String serverPort = args.length > 1 ? args [1] : "54333";

        //Load config resource file and start the ClientActor
        final Config conf = ConfigFactory.load("conf/client.conf");

        final ActorSystem sys = ActorSystem.create("Client", conf);
        final ActorRef client = sys.actorOf(
                ClientActor.props("ServerClusterSystem@"+serverAddress+":"+serverPort), "ClientActor");

        final Scanner scanner = new Scanner(System.in);

        //Loop which handles messages to send to the ClientActor, from which will be sent to the server
        while (true) {
            final String command = scanner.nextLine();
            if (command.equals("quit")) {
                break;
            } else {
                //for each message parsed, send the message to the client actor
                for (ServiceMessage sm :
                        CommandParser.parseLine(command)) {
                    client.tell(sm, ActorRef.noSender());
                }

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
