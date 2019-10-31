# node-rdkafka-commit-manager

A simple helper for controlling when an offset is ready to be committed via node-rdkafka.

# Goals

This package is intended to help you implement at-least-once processing without making a network call for every message you process.

During frequent use, the commit manager will commit offsets only as often as a configurable commit interval.

During infrequent use, the commit manager will always immediately commit if it has seen a period of inactivity exceeding the configurable commit interval.

# useCommitManager

The commit manager is exposed via a hook function (you don't need to be using React in order to use this.)

`const { readyToCommit, onRebalance } = useCommitManager(consumer, commitIntervalMs);`

### Arguments

- consumer: this must be a KafkaConsumer object from node-rdkafka. It must be configured with auto-commit turned off.
- commitIntervalMs: the number of milliseconds the commit manager should wait between commits.
  - optional: defaults to 5000

### Returned Functions

- readyToCommit(data)
  - data: This must be an object containing the properties "topic", "partition", and "offset".
  - Call this whenever you are finished with a Kafka message.
- onRebalance()
  - Call this whenever your KafkaConsumer has its partitions revoked.

# Usage Example

```
import { useCommitManager } from "node-rdkafka-commit-manager";
import { CODES, KafkaConsumer } from "node-rdkafka";

const rebalanceCallback = async (err: any, assignments: any) => {
  if (err.code === CODES.ERRORS.ERR__ASSIGN_PARTITIONS) {
    consumer.assign(assignments);
  } else if (err.code === CODES.ERRORS.ERR__REVOKE_PARTITIONS) {
    consumer.unassign();
    onRebalance();
  } else {
    console.error(`Kafka rebalance error : ${err}`);
  }
};

const consumer = new KafkaConsumer(
  {
    "enable.auto.commit": false,
    rebalance_cb: rebalanceCallback.bind(this),
    <Your global config here (ex: authentication, consumer group, etc.)>
  },
  {
    <Your topic config here>
  }
);

const { readyToCommit, onRebalance } = useCommitManager(consumer);

consumer
  .on("ready", function() {
    consumer.subscribe(["phw.soteria.projects"]);
    consumer.consume();
  })
  .on("data", function(data: any) {
    <Process the Kafka message here.>
    readyToCommit(data);
  })
  .connect();
```

The above example illustrates a few key points about using this commit manager:

1. Disable auto-commit feature on your KafkaConsumer.

   - Otherwise, the commit manager will be competing with node-rdkafka's auto-commit behavior.

1. Implement a rebalance callback function which calls the the commit manager's onRebalance function any time your KafkaConsumer's partitions are revoked.

   - Otherwise, the commit manager may later try to commit offsets for partitions which it is no longer assigned.

   - The above example covers the minimum responsibilities of the function. See the node-rdkafka and/or librdkafka documentation for more details.

1. Call the commit manager's readyToCommit function for each Kafka message you process.

   - Only call readyToCommit when you have finished processing the message.
   - Since the data objects provided by node-rdkafka's KafkaConsumer already have all of the necessary properties, you can just use those if you want to.
