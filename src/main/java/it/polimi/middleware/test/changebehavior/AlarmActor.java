package it.polimi.middleware.test.changebehavior;

import akka.actor.AbstractActor;
import akka.actor.Props;
import it.polimi.middleware.test.changebehavior.messages.ActivateMsg;
import it.polimi.middleware.test.changebehavior.messages.DeactivateMsg;
import it.polimi.middleware.test.changebehavior.messages.TriggerMsg;

public class AlarmActor extends AbstractActor {
	@Override
	public Receive createReceive() {
		return disabled();
	}

	// Behaviors

	private final Receive enabled() {
		return receiveBuilder(). //
		    match(TriggerMsg.class, this::onTrigger). //
		    match(DeactivateMsg.class, this::onDeactivate). //
		    build();
	}

	private final Receive disabled() {
		return receiveBuilder(). //
		    match(ActivateMsg.class, this::onActivate). //
		    build();
	}

	// Message handling

	private final void onTrigger(TriggerMsg msg) {
		System.out.println("Alarm!!!!");
	}

	private final void onActivate(ActivateMsg msg) {
		System.out.println("Becoming enabled");
		getContext().become(enabled());
	}

	private final void onDeactivate(DeactivateMsg msg) {
		System.out.println("Becoming disabled");
		getContext().become(disabled());
	}

	public static final Props props() {
		return Props.create(AlarmActor.class);
	}

}