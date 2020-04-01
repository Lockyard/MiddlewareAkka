package it.polimi.middleware.server;

import akka.actor.ActorSystem;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import it.polimi.middleware.server.actors.MasterNode;

import java.io.File;
import java.util.Scanner;

public class ServerApp {

    public static void main (String[] args) {

        final Config conf = ConfigFactory.parseFile(new File("conf/cluster.conf"));
        final ActorSystem sys = ActorSystem.create("ServerSystem", conf);
        sys.actorOf(MasterNode.props(), "MasterNode");

        System.out.println("Server started, enter 'quit' to terminate");
        Scanner s = new Scanner(System.in);
        String input = "";
        while(!input.equalsIgnoreCase("quit")) {
            input = s.nextLine();
        }
        sys.terminate();
        s.close();
    }
}
