package it.polimi.middleware.server;

import akka.actor.ActorSystem;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import it.polimi.middleware.server.actors.MasterNode;

import java.io.File;
import java.util.Collections;

public class ServerApp {

    public static void main (String[] args) {

        File f = new File("cluster.conf");

        final Config conf = ConfigFactory.parseFile(new File("cluster.conf"));
        final ActorSystem sys = ActorSystem.create("ServerSystem", conf);
        sys.actorOf(MasterNode.props(), "MasterNode");
    }
}
