package bcr.activemq;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


@ExtendWith(SpringExtension.class)
@ContextConfiguration
public class MyMessageProducerTest {

    @Test
    public void testMessaging() throws InterruptedException {
        List<MyMessageConsumer> consumers = new ArrayList<MyMessageConsumer>();
        IntStream.range(0,10).forEach(i ->
                consumers.add(new MyMessageConsumer("tcp://ubuntu1.local:25102/", "TEST.FOO")));
        IntStream.range(0,10).forEach(i ->
                consumers.add(new MyMessageConsumer("tcp://ubuntu1.local:25103/", "TEST.FOO")));

        List<MyMessageProducer> producers = new ArrayList<MyMessageProducer>();
        IntStream.range(0,10).forEach(i ->
                producers.add(new MyMessageProducer("tcp://ubuntu1.local:25100/", "TEST.FOO", 1000)));
        IntStream.range(0,10).forEach(i ->
                producers.add(new MyMessageProducer("tcp://ubuntu1.local:25101/", "TEST.FOO", 1000)));

        ExecutorService executorService = Executors.newFixedThreadPool(consumers.size()+producers.size());
        consumers.forEach(c -> executorService.submit(c));
        producers.forEach(p -> executorService.submit(p));

        System.out.println("Started "+consumers.size()+" consumers.");
        System.out.println("Started "+producers.size()+" producers.");

        Executors.newSingleThreadExecutor().submit(() -> {
            while (true) {
                String consumerCounts = consumers.stream().map(c -> Integer.toString(c.getMessagesReceived())).collect(Collectors.joining(","));
                String producerCounts = producers.stream().map(c -> Integer.toString(c.getMessagesProduced())).collect(Collectors.joining(","));
                System.out.println(producerCounts + " -> " + consumerCounts);
                TimeUnit.SECONDS.sleep(5);
            }
        });
        executorService.awaitTermination(30, TimeUnit.MINUTES);
    }

    @Configuration
    static class Config {
    }
}