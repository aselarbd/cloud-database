package de.tum.i13.client.subscribers;

import de.tum.i13.client.subscription.Subscriber;
import de.tum.i13.shared.KVItem;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.function.Consumer;

@Disabled
public class SubscriberTest {

    @Test
    public void testSubscribe() throws IOException, InterruptedException {
        Subscriber sub = new Subscriber(new InetSocketAddress("localhost", 5153), new Consumer<KVItem>() {
            @Override
            public void accept(KVItem kvItem) {
                System.out.println(kvItem);
            }
        }, s -> {});

        sub.subscribe("Test");
        Thread.sleep(5000);
        sub.unsubscribe("Test");
        sub.subscribe("Test2");
        sub.subscribe("Test3");
        Thread.sleep(50000);
    }
}
