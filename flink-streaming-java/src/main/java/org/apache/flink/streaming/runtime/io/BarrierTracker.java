/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * The BarrierTracker keeps track of what checkpoint barriers have been received from
 * which input channels. Once it has observed all checkpoint barriers for a checkpoint ID,
 * it notifies its listener of a completed checkpoint.
 *
 * <p>Unlike the {@link BarrierBuffer}, the BarrierTracker does not block the input
 * channels that have sent barriers, so it cannot be used to gain "exactly-once" processing
 * guarantees. It can, however, be used to gain "at least once" processing guarantees.
 *
 * <p>NOTE: This implementation strictly assumes that newer checkpoints have higher checkpoint IDs.
 */
@Internal
public class BarrierTracker implements CheckpointBarrierHandler {

	private static final Logger LOG = LoggerFactory.getLogger(BarrierTracker.class);

	/**
	 * The tracker tracks a maximum number of checkpoints, for which some, but not all barriers
	 * have yet arrived.
	 */
	private static final int MAX_CHECKPOINTS_TO_TRACK = 50;

	// ------------------------------------------------------------------------

	/** The input gate, to draw the buffers and events from. */
	private final InputGate inputGate;

	/**
	 * The number of channels. Once that many barriers have been received for a checkpoint, the
	 * checkpoint is considered complete.
	 */
	private final int totalNumberOfInputChannels;

	/**
	 * All checkpoints for which some (but not all) barriers have been received, and that are not
	 * yet known to be subsumed by newer checkpoints.
	 */
	private final ArrayDeque<CheckpointBarrierCount> pendingCheckpoints;

	/** The listener to be notified on complete checkpoints. */
	private AbstractInvokable toNotifyOnCheckpoint;

	/** The highest checkpoint ID encountered so far. */
	private long latestPendingCheckpointID = -1;

	/** The IDs of the checkpoint for which we are notified aborted. */
	private final NavigableSet<Long> abortedCheckpointIds;

	// ------------------------------------------------------------------------

	public BarrierTracker(InputGate inputGate) {
		this.inputGate = inputGate;
		this.totalNumberOfInputChannels = inputGate.getNumberOfInputChannels();
		this.pendingCheckpoints = new ArrayDeque<>();
		this.abortedCheckpointIds = new ConcurrentSkipListSet();
	}

	@Override
	public CompletableFuture<?> isAvailable() {
		return inputGate.isAvailable();
	}

	@Override
	public boolean isFinished() {
		return inputGate.isFinished();
	}

	@Override
	public Optional<BufferOrEvent> pollNext() throws Exception {
		while (true) {
			Optional<BufferOrEvent> next = inputGate.pollNext();
			if (!next.isPresent()) {
				// buffer or input exhausted
				return next;
			}

			BufferOrEvent bufferOrEvent = next.get();
			if (bufferOrEvent.isBuffer()) {
				return next;
			}
			else if (bufferOrEvent.getEvent().getClass() == CheckpointBarrier.class) {
				processBarrier((CheckpointBarrier) bufferOrEvent.getEvent(), bufferOrEvent.getChannelIndex());
			}
			else {
				// some other event
				return next;
			}
		}
	}

	@Override
	public void registerCheckpointEventHandler(AbstractInvokable toNotifyOnCheckpoint) {
		if (this.toNotifyOnCheckpoint == null) {
			this.toNotifyOnCheckpoint = toNotifyOnCheckpoint;
		}
		else {
			throw new IllegalStateException("BarrierTracker already has a registered checkpoint notifyee");
		}
	}

	@Override
	public void cleanup() {
		pendingCheckpoints.clear();
	}

	@Override
	public boolean isEmpty() {
		return pendingCheckpoints.isEmpty();
	}

	@Override
	public long getAlignmentDurationNanos() {
		// this one does not do alignment at all
		return 0L;
	}

	@Override
	public int getNumberOfInputChannels() {
		return totalNumberOfInputChannels;
	}

	@Override
	public void abortCheckpoint(long checkpointId) {
		if (checkpointId >= latestPendingCheckpointID) {
			abortedCheckpointIds.add(checkpointId);
			synchronized (abortedCheckpointIds) {
				Iterator<Long> iterator = abortedCheckpointIds.iterator();
				while (iterator.hasNext()) {
					if (iterator.next() < latestPendingCheckpointID) {
						iterator.remove();
					} else {
						break;
					}
				}
				while (abortedCheckpointIds.size() > MAX_CHECKPOINTS_TO_TRACK) {
					abortedCheckpointIds.pollFirst();
				}
			}
		}
	}

	private void processBarrier(CheckpointBarrier receivedBarrier, int channelIndex) throws Exception {
		final long barrierId = receivedBarrier.getId();

		// fast path for single channel trackers
		if (totalNumberOfInputChannels == 1) {
			if (!abortedCheckpointIds.contains(barrierId)) {
				notifyCheckpoint(barrierId, receivedBarrier.getTimestamp(), receivedBarrier.getCheckpointOptions());
			}
			return;
		}

		// general path for multiple input channels
		if (LOG.isDebugEnabled()) {
			LOG.debug("Received barrier for checkpoint {} from channel {}", barrierId, channelIndex);
		}

		if (abortedCheckpointIds.contains(barrierId)) {
			processAbortedCheckpoint(barrierId);
			return;
		}
		// find the checkpoint barrier in the queue of pending barriers
		CheckpointBarrierCount cbc = null;
		int pos = 0;

		for (CheckpointBarrierCount next : pendingCheckpoints) {
			if (next.checkpointId == barrierId) {
				cbc = next;
				break;
			}
			pos++;
		}

		if (cbc != null) {
			// add one to the count to that barrier and check for completion
			int numBarriersNew = cbc.incrementBarrierCount();
			if (numBarriersNew == totalNumberOfInputChannels) {
				// checkpoint can be triggered (or is aborted and all barriers have been seen)
				// first, remove this checkpoint and all all prior pending
				// checkpoints (which are now subsumed)
				for (int i = 0; i <= pos; i++) {
					pendingCheckpoints.pollFirst();
				}

				// notify the listener
				if (!cbc.isAborted()) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Received all barriers for checkpoint {}", barrierId);
					}

					notifyCheckpoint(receivedBarrier.getId(), receivedBarrier.getTimestamp(), receivedBarrier.getCheckpointOptions());
				}
			}
		}
		else {
			// first barrier for that checkpoint ID
			// add it only if it is newer than the latest checkpoint.
			// if it is not newer than the latest checkpoint ID, then there cannot be a
			// successful checkpoint for that ID anyways
			if (barrierId > latestPendingCheckpointID) {
				latestPendingCheckpointID = barrierId;
				pendingCheckpoints.addLast(new CheckpointBarrierCount(barrierId));

				// make sure we do not track too many checkpoints
				if (pendingCheckpoints.size() > MAX_CHECKPOINTS_TO_TRACK) {
					pendingCheckpoints.pollFirst();
				}
			}
		}
	}

	private void processAbortedCheckpoint(long checkpointId) throws Exception {
		// -- general path for multiple input channels --

		// find the checkpoint barrier in the queue of pending barriers
		// while doing this we "abort" all checkpoints before that one
		CheckpointBarrierCount cbc;
		while ((cbc = pendingCheckpoints.peekFirst()) != null && cbc.checkpointId() <= checkpointId) {
			pendingCheckpoints.removeFirst();
		}

		if (checkpointId > latestPendingCheckpointID) {
			latestPendingCheckpointID = checkpointId;

			// we have removed all other pending checkpoint barrier counts --> no need to check that
			// we don't exceed the maximum checkpoints to track
		} else {
			// trailing cancellation barrier which was already cancelled
		}
	}

	private void notifyCheckpoint(long checkpointId, long timestamp, CheckpointOptions checkpointOptions) throws Exception {
		if (toNotifyOnCheckpoint != null) {
			CheckpointMetaData checkpointMetaData = new CheckpointMetaData(checkpointId, timestamp);
			CheckpointMetrics checkpointMetrics = new CheckpointMetrics()
				.setBytesBufferedInAlignment(0L)
				.setAlignmentDurationNanos(0L);

			toNotifyOnCheckpoint.triggerCheckpointOnBarrier(checkpointMetaData, checkpointOptions, checkpointMetrics);
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * Simple class for a checkpoint ID with a barrier counter.
	 */
	private static final class CheckpointBarrierCount {

		private final long checkpointId;

		private int barrierCount;

		private boolean aborted;

		CheckpointBarrierCount(long checkpointId) {
			this.checkpointId = checkpointId;
			this.barrierCount = 1;
		}

		public long checkpointId() {
			return checkpointId;
		}

		public int incrementBarrierCount() {
			return ++barrierCount;
		}

		public boolean isAborted() {
			return aborted;
		}

		public boolean markAborted() {
			boolean firstAbort = !this.aborted;
			this.aborted = true;
			return firstAbort;
		}

		@Override
		public String toString() {
			return isAborted() ?
				String.format("checkpointID=%d - ABORTED", checkpointId) :
				String.format("checkpointID=%d, count=%d", checkpointId, barrierCount);
		}
	}
}
