package it.polimi.middleware.server;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import it.polimi.middleware.server.actors.StoreNode;
import it.polimi.middleware.util.Logger;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * An app which can spawn StoreNodes which will connect to the store and work for it
 */
public class StoreNodeRunner {

    private static ActorSystem actorSystem;

    private static int stdTimeout;

    public static void main(String[] args) {

        if (args.length == 0) {
            startupStoreNodes(Collections.singletonList("9001"));
        } else {
            startupStoreNodes(Arrays.asList(args));
        }

        /*/
        //Load config resource file and start the StoreNodeHost ActorSystem
        final Config conf = ConfigFactory.load("conf/cluster.conf");

        actorSystem = ActorSystem.create("StoreNodeHost", conf);

        final Config storeConf = ConfigFactory.load("conf/store.conf");
        stdTimeout = storeConf.getInt("store.connection.stdTimeout");


        actorSystem.terminate();
        //*/
    }

    private static void startupStoreNodes(List<String> ports) {
        ports.forEach(port -> {
            Logger.std.dlog("starting up node at port " + port);
            ActorSystem actorSystem = ActorSystem.create("ServerClusterSystem", setupStoreNodeConfig(port));
            //create the store node
            ActorRef a = actorSystem.actorOf(StoreNode.props(ConfigFactory.load("conf/store.conf").getInt("store.connection.stdTimeout")), "storenode");
            Logger.std.dlog("new node path: " +a.path());
        });
    }

    private static Config setupStoreNodeConfig(String port) {
        return ConfigFactory.load("conf/cluster.conf")
                .withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(port)) //
                .withValue("akka.remote.artery.canonical.port", ConfigValueFactory.fromAnyRef(port))
                .withValue("akka.cluster.roles", ConfigValueFactory.fromIterable(Collections.singletonList("storenode")));
    }



}
