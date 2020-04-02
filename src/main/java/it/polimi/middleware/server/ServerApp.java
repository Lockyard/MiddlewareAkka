package it.polimi.middleware.server;

import akka.actor.ActorSystem;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import it.polimi.middleware.server.actors.MasterNode;

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
            sys.actorOf(MasterNode.props(), "MasterNode");

            //simple loop to handle basic commands.
            System.out.println("Server started, enter 'quit' to terminate");
            Scanner s = new Scanner(System.in);
            String input = "";
            while(!input.equalsIgnoreCase("quit")) {
                input = s.nextLine();
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
