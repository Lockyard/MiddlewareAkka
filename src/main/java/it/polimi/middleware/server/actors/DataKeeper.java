package it.polimi.middleware.server.actors;

import akka.actor.AbstractActor;
import akka.actor.Props;
import it.polimi.middleware.messages.GetMsg;
import it.polimi.middleware.messages.PutMsg;
import it.polimi.middleware.messages.ReplyGetMsg;
import it.polimi.middleware.server.store.ValueData;

import java.util.HashMap;

/**
 * Actor directed by a store node. Manages the actual operations of store/update of values.
 * Since a StoreNode can have more than one data keeper, the subspace assigned to him could be arbitrarily subdivided.
 * Thus, each DataKeeper see a hashSpacePartition more divided than the one seen by the Store Node.
 */
public class DataKeeper extends AbstractActor {

    private final int hashSpacePartition;
    private final int partitionNumber;

    private final HashMap<String, ValueData> data;

    public DataKeeper(int hashSpacePartition, int partitionNumber) {
        this.hashSpacePartition = hashSpacePartition;
        this.partitionNumber = partitionNumber;

        data = new HashMap<>();
    }


    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(GetMsg.class, this::onGetMsg)
                .match(PutMsg.class, this::onPutMsg)
                .build();
    }

    private void onGetMsg(GetMsg msg) {
        ValueData keyData = data.get(msg.getKey());
        sender().tell(new ReplyGetMsg(keyData.getContent()), self());
    }

    private void onPutMsg(PutMsg msg) {

    }

    public static Props props(int hashSpacePartition, int partitionNumber) {
        return Props.create(DataKeeper.class, hashSpacePartition, partitionNumber);
    }
}
