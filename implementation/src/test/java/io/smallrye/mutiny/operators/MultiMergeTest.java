package io.smallrye.mutiny.operators;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.Test;

import io.reactivex.Flowable;
import io.smallrye.mutiny.CompositeException;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.spies.MultiOnCancellationSpy;
import io.smallrye.mutiny.helpers.spies.Spy;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;

public class MultiMergeTest {

    @Test
    public void testMergeNothing() {
        AssertSubscriber<?> subscriber = Multi.createBy().merging().streams()
                .subscribe()
                .withSubscriber(new AssertSubscriber<>(100));

        subscriber.assertCompleted()
                .assertHasNotReceivedAnyItem();
    }

    @Test
    public void testMergeOne() {
        AssertSubscriber<Integer> subscriber = Multi.createBy().merging().streams(Multi.createFrom().items(1, 2, 3))
                .subscribe()
                .withSubscriber(new AssertSubscriber<>(100));

        subscriber.assertCompleted()
                .assertItems(1, 2, 3);
    }

    @Test
    public void testMergeOneButEmpty() {
        AssertSubscriber<?> subscriber = Multi.createBy().merging().streams(Multi.createFrom().empty())
                .subscribe()
                .withSubscriber(new AssertSubscriber<>(100));

        subscriber.assertCompleted()
                .assertHasNotReceivedAnyItem();
    }

    @Test
    public void testMergeOneButNever() {
        MultiOnCancellationSpy<Object> spy = Spy.onCancellation(Multi.createFrom().nothing());
        AssertSubscriber<?> subscriber = Multi.createBy().merging().streams(spy)
                .subscribe()
                .withSubscriber(new AssertSubscriber<>(100));

        subscriber.assertNotTerminated()
                .cancel();

        assertThat(spy.isCancelled()).isTrue();
    }

    @Test
    public void testMergeOfSeveralMultis() {
        AssertSubscriber<Integer> subscriber = Multi.createBy().merging().streams(
                Multi.createFrom().item(5),
                Multi.createFrom().range(1, 3),
                Multi.createFrom().items(8, 9, 10).onItem().transform(i -> i + 1)).subscribe()
                .withSubscriber(new AssertSubscriber<>(100));

        subscriber.assertCompleted()
                .assertItems(5, 1, 2, 9, 10, 11);
    }

    @Test
    public void testMergeOfSeveralMultisWithConcurrencyAndRequests() {
        AssertSubscriber<Integer> subscriber = Multi.createBy().merging().withConcurrency(2).withRequests(1)
                .streams(
                        Multi.createFrom().item(5),
                        Multi.createFrom().range(1, 3),
                        Multi.createFrom().items(8, 9, 10).onItem().transform(i -> i + 1))
                .subscribe().withSubscriber(new AssertSubscriber<>(100));

        subscriber.assertCompleted()
                .assertItems(5, 1, 2, 9, 10, 11);
    }

    @Test
    public void testMergeOfSeveralMultisAsIterable() {
        final List<Multi<Integer>> multis = Arrays.asList(
                Multi.createFrom().item(5),
                Multi.createFrom().range(1, 3),
                Multi.createFrom().items(8, 9, 10).onItem().transform(i -> i + 1));
        AssertSubscriber<Integer> subscriber = Multi.createBy().merging().streams(multis)
                .subscribe().withSubscriber(new AssertSubscriber<>(100));

        subscriber.assertCompleted()
                .assertItems(5, 1, 2, 9, 10, 11);
    }

    @Test
    public void testMergeOfSeveralPublishers() {
        AssertSubscriber<Integer> subscriber = Multi.createBy().merging().streams(
                Flowable.just(5),
                Multi.createFrom().range(1, 3),
                Multi.createFrom().items(8, 9, 10).onItem().transform(i -> i + 1)).subscribe()
                .withSubscriber(new AssertSubscriber<>(100));

        subscriber.assertCompleted()
                .assertItems(5, 1, 2, 9, 10, 11);
    }

    @Test
    public void testMergeOfSeveralPublishersAsIterable() {
        AssertSubscriber<Integer> subscriber = Multi.createBy().merging().streams(
                Arrays.asList(
                        Flowable.just(5),
                        Multi.createFrom().range(1, 3),
                        Multi.createFrom().items(8, 9, 10).onItem().transform(i -> i + 1)))
                .subscribe().withSubscriber(new AssertSubscriber<>(100));

        subscriber.assertCompleted()
                .assertItems(5, 1, 2, 9, 10, 11);
    }

    @Test
    public void testMergingEmpty() {
        Multi.createBy().merging().streams(Multi.createFrom().empty())
                .subscribe().withSubscriber(AssertSubscriber.create(1))
                .assertCompleted().assertHasNotReceivedAnyItem();
    }

    @Test
    public void testMergingWithEmpty() {
        Multi.createBy().merging().streams(Multi.createFrom().empty(), Multi.createFrom().item(2))
                .subscribe().withSubscriber(AssertSubscriber.create(1))
                .assertCompleted().assertItems(2);
    }

    @Test
    public void testWithFailureCollectionWithConcatenation() {
        IllegalStateException boom = new IllegalStateException("boom");
        IllegalStateException boom2 = new IllegalStateException("boom2");

        AssertSubscriber<Integer> subscriber = Multi.createBy().concatenating().collectFailures().streams(
                Multi.createFrom().item(5),
                Multi.createFrom().failure(boom),
                Multi.createFrom().item(6),
                Multi.createFrom().failure(boom2)).subscribe().withSubscriber(new AssertSubscriber<>(5));

        subscriber.assertTerminated()
                .assertItems(5, 6)
                .assertFailedWith(CompositeException.class, "boom")
                .assertFailedWith(CompositeException.class, "boom2");

        assertThat(subscriber.getFailure()).isInstanceOf(CompositeException.class);
        CompositeException ce = (CompositeException) subscriber.getFailure();
        assertThat(ce.getCauses()).hasSize(2);

        subscriber = Multi.createBy().concatenating().streams(
                Multi.createFrom().item(5),
                Multi.createFrom().failure(boom),
                Multi.createFrom().item(6),
                Multi.createFrom().failure(boom)).subscribe().withSubscriber(new AssertSubscriber<>(5));

        subscriber.assertTerminated()
                .assertItems(5)
                .assertFailedWith(IllegalStateException.class, "boom");

    }

    @Test
    public void testWithFailureCollectionWithMerge() {
        IllegalStateException boom = new IllegalStateException("boom");
        IllegalStateException boom2 = new IllegalStateException("boom2");

        AssertSubscriber<Integer> subscriber = Multi.createBy().merging().collectFailures().streams(
                Multi.createFrom().item(5),
                Multi.createFrom().failure(boom),
                Multi.createFrom().item(6),
                Multi.createFrom().failure(boom2)).subscribe().withSubscriber(new AssertSubscriber<>(5));

        subscriber.assertTerminated()
                .assertItems(5, 6)
                .assertFailedWith(CompositeException.class, "boom")
                .assertFailedWith(CompositeException.class, "boom2");

        assertThat(subscriber.getFailure()).isInstanceOf(CompositeException.class);
        CompositeException ce = (CompositeException) subscriber.getFailure();
        assertThat(ce.getCauses()).hasSize(2);

        subscriber = Multi.createBy().merging().streams(
                Multi.createFrom().item(5),
                Multi.createFrom().failure(boom),
                Multi.createFrom().item(6),
                Multi.createFrom().failure(boom)).subscribe().withSubscriber(new AssertSubscriber<>(5));

        subscriber.assertTerminated()
                .assertItems(5)
                .assertFailedWith(IllegalStateException.class, "boom");

    }

    private Multi<String> createMultiWithTicks(String name) {
        final AtomicLong counter = new AtomicLong();
        return Multi.createFrom().ticks().every(Duration.ofMillis(100))
                .onOverflow().drop()
                .flatMap(aLong -> Multi.createFrom().range(0, 400).map(integer -> counter.getAndIncrement()))
                .map(aLong -> name + " " + aLong)
                .onRequest().invoke(value -> System.out.printf("Requested %d from %s\n", value, name));
    }

    @Test
    void testFairMerge() {
        Multi<String> first = createMultiWithTicks("First");
        Multi<String> second = createMultiWithTicks("Second");

        AtomicLong firstCounter = new AtomicLong();
        AtomicLong secondCounter = new AtomicLong();
        AssertSubscriber<String> assertSubscriber = Multi.createBy().merging()
                .withBatch(30)
                .streams(first, second)
                .onItem().invoke(aLong -> {
                    if (aLong.startsWith("First")) {
                        firstCounter.incrementAndGet();
                    } else {
                        secondCounter.incrementAndGet();
                    }
                }).subscribe().withSubscriber(new AssertSubscriber<>(Long.MAX_VALUE));

        assertSubscriber.awaitItems(50000, Duration.ofSeconds(60));
        long firstC = firstCounter.get();
        long secondC = secondCounter.get();
        System.out.println("Count First " + firstC);
        System.out.println("Count Second " + secondC);
        assertThat(firstC).isCloseTo(secondC, Percentage.withPercentage(10));
    }

    @Test
    void testFairMerge2() {
        Multi<String> first = createMultiWithTicks("First");
        Multi<String> second = createMultiWithTicks("Second");

        AtomicLong firstCounter = new AtomicLong();
        AtomicLong secondCounter = new AtomicLong();
        AssertSubscriber<String> assertSubscriber = Multi.createBy().merging()
                .withBatch(30).withRequests(30)
                .streams(first, second)
                .onItem().invoke(aLong -> {
                    if (aLong.startsWith("First")) {
                        firstCounter.incrementAndGet();
                    } else {
                        secondCounter.incrementAndGet();
                    }
                })
                        .subscribe().withSubscriber(new AssertSubscriber<String>(1) {
                            @Override
                            public synchronized void onNext(String o) {
                                super.onNext(o);
                                request(1);
                            }
                        });

        assertSubscriber.awaitItems(50000, Duration.ofSeconds(60));
        long firstC = firstCounter.get();
        long secondC = secondCounter.get();
        System.out.println("Count First " + firstC);
        System.out.println("Count Second " + secondC);
        assertThat(firstC).isCloseTo(secondC, Percentage.withPercentage(10));
    }
}
