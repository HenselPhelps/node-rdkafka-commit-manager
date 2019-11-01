import { KafkaConsumer } from "node-rdkafka";

interface OffsetMap {
  // partition is converted to a string here, because object keys must be strings.
  [topic: string]: { [partition: string]: number };
}

export interface OffsetDescriptor {
  topic: string;
  partition: number;
  offset: number;
}

export const useCommitManager = (
  consumer: KafkaConsumer,
  commitIntervalMs = 5000
) => {
  let readyOffsets: OffsetMap = {};
  let lastCommitTimestamp = 0;
  let commitIntervalId: NodeJS.Timeout | null = null;

  const readyToCommit = (data: OffsetDescriptor) => {
    if (!readyOffsets[data.topic]) {
      readyOffsets[data.topic] = {};
    }
    readyOffsets[data.topic][data.partition.toString()] = data.offset;
    if (lastCommitTimestamp < Date.now() - commitIntervalMs) {
      if (commitIntervalId) {
        clearInterval(commitIntervalId);
        commitIntervalId = null;
      }
      commitReadyOffsets();
    } else {
      if (!commitIntervalId) {
        commitIntervalId = setInterval(() => {
          commitReadyOffsets();
        }, commitIntervalMs);
      }
    }
  };

  function commitReadyOffsets() {
    const offsetsToCommit = Object.entries(readyOffsets).reduce(
      (accumulator: OffsetDescriptor[], [topic, partitionOffsetMap]) => {
        const partitionOffsetDescriptors: OffsetDescriptor[] = Object.entries(
          partitionOffsetMap
        ).map(([partition, offset]) => ({
          topic: topic,
          partition: parseInt(partition),
          offset: offset
        }));
        return accumulator.concat(partitionOffsetDescriptors);
      },
      []
    );
    if (offsetsToCommit.length) consumer.commit(offsetsToCommit);
    readyOffsets = {};
    lastCommitTimestamp = Date.now();
  }

  const onRebalance = () => {
    commitReadyOffsets();
  };

  return { readyToCommit, onRebalance };
};
