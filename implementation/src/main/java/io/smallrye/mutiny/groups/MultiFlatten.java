package io.smallrye.mutiny.groups;

import static io.smallrye.mutiny.helpers.ParameterValidation.positive;

import java.util.function.Function;

import io.smallrye.mutiny.operators.multi.MultiFlatMapWithBatchOp;
import org.reactivestreams.Publisher;

import io.smallrye.common.annotation.CheckReturnValue;
import io.smallrye.mutiny.CompositeException;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.queues.Queues;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.operators.multi.MultiFlatMapOp;

/**
 * The object to tune the <em>flatMap</em> operation
 *
 * @param <I> the type of item emitted by the upstream {@link Multi}
 * @param <O> the type of item emitted by the returned {@link Multi}
 */
public class MultiFlatten<I, O> {

    private final Function<? super I, ? extends Publisher<? extends O>> mapper;
    private final Multi<I> upstream;

    private final int requests;
    private final int batch;
    private final boolean collectFailureUntilCompletion;

    MultiFlatten(Multi<I> upstream,
            Function<? super I, ? extends Publisher<? extends O>> mapper,
            int requests, boolean collectFailures, int batch) {
        this.upstream = upstream;
        this.mapper = mapper;
        this.requests = requests;
        this.batch = batch;
        this.collectFailureUntilCompletion = collectFailures;
    }

    /**
     * Instructs the <em>flatMap</em> operation to consume all the <em>streams</em> returned by the mapper before
     * propagating a failure if any of the <em>stream</em> has produced a failure.
     * <p>
     * If more than one failure is collected, the propagated failure is a
     * {@link CompositeException}.
     *
     * @return this {@link MultiFlatten}
     */
    @CheckReturnValue
    public MultiFlatten<I, O> collectFailures() {
        return new MultiFlatten<>(upstream, mapper, requests, true, batch);
    }

    /**
     * Configures the number the items requested to the <em>streams</em> produced by the mapper.
     *
     * @param requests the requests, must be strictly positive
     * @return this {@link MultiFlatten}
     */
    @CheckReturnValue
    public MultiFlatten<I, O> withRequests(int requests) {
        return new MultiFlatten<>(upstream, mapper, positive(requests, "requests"), collectFailureUntilCompletion, this.batch);
    }

    /**
     * Indicates that the inner stream consumption uses batch (TODO IMPROVE THIS)
     *
     * @param batch the batch size, must be strictly positive
     * @return this {@link MultiFlatten}
     */
    @CheckReturnValue
    public MultiFlatten<I, O> withBatch(int batch) {
        return new MultiFlatten<>(upstream, mapper, positive(requests, "requests"), collectFailureUntilCompletion, batch);
    }

    /**
     * Produces a {@link Multi} containing the items from {@link Publisher} produced by the {@code mapper} for each
     * item emitted by this {@link Multi}.
     * <p>
     * The operators behaves as follows:
     * <ul>
     * <li>for each item emitted by this {@link Multi}, the mapper is called and produces a {@link Publisher}
     * (potentially a {@code Multi}). The mapper must not return {@code null}</li>
     * <li>The items contained in each of the produced {@link Publisher} are then <strong>merged</strong> in the
     * produced {@link Multi}. The returned object lets you configure the flattening process.</li>
     * </ul>
     *
     * @return the object to configure the {@code flatMap} operation.
     */
    @CheckReturnValue
    public Multi<O> merge() {
        return merge(Queues.BUFFER_S);
    }

    /**
     * Produces a {@link Multi} containing the items from {@link Publisher} produced by the {@code mapper} for each
     * item emitted by this {@link Multi}.
     * <p>
     * The operators behaves as follows:
     * <ul>
     * <li>for each item emitted by this {@link Multi}, the mapper is called and produces a {@link Publisher}
     * (potentially a {@code Multi}). The mapper must not return {@code null}</li>
     * <li>The items contained in each of the produced {@link Publisher} are then <strong>merged</strong> in the
     * produced {@link Multi}. The returned object lets you configure the flattening process.</li>
     * </ul>
     * <p>
     * This method allows configuring the concurrency, i.e. the maximum number of in-flight/subscribed inner streams
     *
     * @param concurrency the concurrency
     * @return the object to configure the {@code flatMap} operation.
     */
    @CheckReturnValue
    public Multi<O> merge(int concurrency) {
        if (batch > 0) {
            return Infrastructure.onMultiCreation(
                    new MultiFlatMapWithBatchOp<>(upstream, mapper, collectFailureUntilCompletion, concurrency, requests, batch));
        }
        return Infrastructure.onMultiCreation(
                new MultiFlatMapOp<>(upstream, mapper, collectFailureUntilCompletion, concurrency, requests));
    }

    /**
     * Produces a {@link Multi} containing the items from {@link Publisher} produced by the {@code mapper} for each
     * item emitted by this {@link Multi}.
     * <p>
     * The operators behaves as follows:
     * <ul>
     * <li>for each item emitted by this {@link Multi}, the mapper is called and produces a {@link Publisher}
     * (potentially a {@code Multi}). The mapper must not return {@code null}</li>
     * <li>The items contained in each of the produced {@link Publisher} are then <strong>concatenated</strong> in the
     * produced {@link Multi}. The returned object lets you configure the flattening process.</li>
     * </ul>
     *
     * @return the object to configure the {@code concatMap} operation.
     */
    @CheckReturnValue
    public Multi<O> concatenate() {
        if (batch != 0) {
            throw new IllegalArgumentException("Cannot concatenate using batches");
        }
        return Infrastructure.onMultiCreation(
                new MultiFlatMapOp<>(upstream, mapper, collectFailureUntilCompletion, 1, requests));
    }
}
