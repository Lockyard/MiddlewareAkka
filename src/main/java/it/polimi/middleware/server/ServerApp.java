package it.polimi.middleware.server;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import it.polimi.middleware.server.actors.MasterNode;
import it.polimi.middleware.server.messages.StartSystemMsg;
import it.polimi.middleware.util.Logger;

import java.util.Collections;
import java.util.Scanner;

/**
 * Main class for server. Starts up the master node and its actorSystem.
 */
public class ServerApp {

    public static void main (String[] args) {
        try {

            final String port = args.length == 0 ? "54333" : args[0];
            //load config file of cluster and start MasterNode
            final Config conf = ConfigFactory.load("conf/cluster.conf")
                    .withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(port)) //
                    .withValue("akka.cluster.roles", ConfigValueFactory.fromIterable(Collections.singletonList("master")));

            final ActorSystem sys = ActorSystem.create("ServerClusterSystem", conf);
            ActorRef masterNode = sys.actorOf(MasterNode.props(), "MasterNode");

            //simple loop to handle basic commands.
            System.out.println("Server started but store is not up by default.\n" +
                    "Enter 'help', or 'h' to get a command list, 'quit' to exit.");
            Scanner s = new Scanner(System.in);
            String input = "";
            while(!input.equalsIgnoreCase("quit")) {
                input = s.nextLine();
                //check commands
                if(input.equalsIgnoreCase("start") || input.equalsIgnoreCase("s")) {
                    masterNode.tell(new StartSystemMsg(false), ActorRef.noSender());
                } else if(input.equalsIgnoreCase("fstart") || input.equalsIgnoreCase("fs")) {
                    masterNode.tell(new StartSystemMsg(true), ActorRef.noSender());
                }  else if(input.equalsIgnoreCase("help") || input.equalsIgnoreCase("h")) {
                    Logger.std.ilog("'start' or 's' -> try start the server if there are enough nodes\n" +
                            "'fstart' or 'fs' -> force the local creation of enough nodes to start and start the server\n" +
                            //"'addnode' or 'an' -> add a new node locally" +
                            "'quit' -> terminate server");
                }
            }

            //quit command entered, quit server
            sys.terminate();
            s.close();


        } catch (Exception e) {
            System.out.println("An error in ServerApp occurred:\n");
            e.printStackTrace();

            System.exit(-1);
        }

    }
}
