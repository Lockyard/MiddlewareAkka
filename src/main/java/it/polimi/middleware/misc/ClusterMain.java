package it.polimi.middleware.misc;

import akka.actor.ActorSystem;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import java.io.File;

public class ClusterMain {

	public static void main(String[] args) {
		final String[] ports = args.length == 0 ? new String[] { "9000", "9001", "0" } : args;
		startup(ports);
	}

	public static void startup(String[] ports) {
		for (final String port : ports) {
			// Override the configuration of the port
			final Config config = ConfigFactory //
			    .parseFile(new File("conf/cluster.conf")) //
			    .withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(port));

			// Create an Akka system
			final ActorSystem system = ActorSystem.create("ClusterSystem", config);

			// Create an actor that handles cluster domain events
			system.actorOf(ClusterListener.props(), "clusterListener");
		}
	}
}