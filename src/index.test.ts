import { useCommitManager } from ".";
import { KafkaConsumer } from "node-rdkafka";

jest.mock("node-rdkafka");

describe("useCommitManager", () => {
  const SHORT_WAIT = 2;
  const LONGER_WAIT = 6;
  const offsetDescription1 = { topic: "abc", partition: 1, offset: 5 };
  const message1 = { ...offsetDescription1, value: "test1" };
  const offsetDescription2 = { topic: "def", partition: 2, offset: 2 };
  const message2 = { ...offsetDescription2, value: "test2" };
  const offsetDescription3 = { topic: "ghi", partition: 3, offset: 4 };
  const message3 = { ...offsetDescription3, value: "test3" };
  const offsetDescription4 = { topic: "def", partition: 2, offset: 3 };
  const message4 = { ...offsetDescription4, value: "test4" };

  it("only commits the first offset immediately", () => {
    // arrange
    const commit = jest.fn();
    const consumer = new KafkaConsumer({}, {});
    consumer.commit = commit;
    const { readyToCommit } = useCommitManager(consumer);

    // act
    readyToCommit(message1);
    readyToCommit(message2);
    readyToCommit(message3);

    // assert
    expect(consumer.commit).toHaveBeenCalledTimes(1);
    expect(consumer.commit).toHaveBeenCalledWith([offsetDescription1]);
  });

  it("only commits once if there are no further ready offsets", async () => {
    // arrange
    const commit = jest.fn();
    const consumer = new KafkaConsumer({}, {});
    consumer.commit = commit;
    const { readyToCommit } = useCommitManager(consumer);

    // act
    readyToCommit(message1);
    await new Promise(resolve => setTimeout(resolve, LONGER_WAIT + SHORT_WAIT));

    // assert
    expect(consumer.commit).toHaveBeenCalledTimes(1);
    expect(consumer.commit).toHaveBeenCalledWith([offsetDescription1]);
  });

  it("commits the second offset immediately if the commit interval has already been exceeded", async () => {
    // arrange
    const commit = jest.fn();
    const consumer = new KafkaConsumer({}, {});
    consumer.commit = commit;
    const { readyToCommit } = useCommitManager(consumer, LONGER_WAIT);

    // act
    readyToCommit(message1);
    await new Promise(resolve => setTimeout(resolve, LONGER_WAIT + SHORT_WAIT));
    readyToCommit(message2);
    readyToCommit(message3);

    // assert
    expect(consumer.commit).toHaveBeenCalledTimes(2);
    expect(consumer.commit).toHaveBeenCalledWith([offsetDescription1]);
    expect(consumer.commit).toHaveBeenCalledWith([offsetDescription2]);
  });

  it("commits all remaining offsets once the commit interval has been exceeded", async () => {
    // arrange
    const commit = jest.fn();
    const consumer = new KafkaConsumer({}, {});
    consumer.commit = commit;
    const { readyToCommit } = useCommitManager(consumer, LONGER_WAIT);

    // act
    readyToCommit(message1);
    readyToCommit(message2);
    readyToCommit(message3);
    await new Promise(resolve => setTimeout(resolve, LONGER_WAIT + SHORT_WAIT));

    // assert
    expect(consumer.commit).toHaveBeenCalledTimes(2);
    expect(consumer.commit).toHaveBeenNthCalledWith(1, [offsetDescription1]);
    expect(consumer.commit).toHaveBeenCalledWith([
      offsetDescription2,
      offsetDescription3
    ]);
  });

  it("commits only the latest offset for a given topic/partition once the commit interval has been exceeded", async () => {
    // arrange
    const commit = jest.fn();
    const consumer = new KafkaConsumer({}, {});
    consumer.commit = commit;
    const { readyToCommit } = useCommitManager(consumer, LONGER_WAIT);

    // act
    readyToCommit(message1);
    readyToCommit(message2);
    readyToCommit(message3);
    readyToCommit(message4);
    await new Promise(resolve => setTimeout(resolve, LONGER_WAIT + SHORT_WAIT));

    // assert
    expect(consumer.commit).toHaveBeenCalledTimes(2);
    expect(consumer.commit).toHaveBeenNthCalledWith(1, [offsetDescription1]);
    expect(consumer.commit).toHaveBeenCalledWith([
      offsetDescription4,
      offsetDescription3
    ]);
  });

  it("does not immediately commit the third message if the commit interval was exceeded between the second and third messages", async () => {
    // arrange
    const commit = jest.fn();
    const consumer = new KafkaConsumer({}, {});
    consumer.commit = commit;
    const { readyToCommit } = useCommitManager(consumer, LONGER_WAIT);

    // act
    readyToCommit(message1);
    readyToCommit(message2);
    await new Promise(resolve => setTimeout(resolve, LONGER_WAIT + SHORT_WAIT));
    readyToCommit(message3);

    // assert
    expect(consumer.commit).toHaveBeenCalledTimes(2);
    expect(consumer.commit).toHaveBeenNthCalledWith(1, [offsetDescription1]);
    expect(consumer.commit).toHaveBeenNthCalledWith(2, [offsetDescription2]);
  });

  it("commits the third message once the second commit interval has been exceeded, if the first commit interval was exceeded between the second and third messages", async () => {
    // arrange
    const commit = jest.fn();
    const consumer = new KafkaConsumer({}, {});
    consumer.commit = commit;
    const { readyToCommit } = useCommitManager(consumer, LONGER_WAIT);

    // act
    readyToCommit(message1);
    readyToCommit(message2);
    await new Promise(resolve => setTimeout(resolve, LONGER_WAIT + SHORT_WAIT));
    readyToCommit(message3);
    await new Promise(resolve => setTimeout(resolve, LONGER_WAIT));

    // assert
    expect(consumer.commit).toHaveBeenCalledTimes(3);
    expect(consumer.commit).toHaveBeenNthCalledWith(1, [offsetDescription1]);
    expect(consumer.commit).toHaveBeenNthCalledWith(2, [offsetDescription2]);
    expect(consumer.commit).toHaveBeenNthCalledWith(3, [offsetDescription3]);
  });

  it("commits all remaining offsets on rebalance", async () => {
    // arrange
    const commit = jest.fn();
    const consumer = new KafkaConsumer({}, {});
    consumer.commit = commit;
    const { readyToCommit, onRebalance } = useCommitManager(
      consumer,
      SHORT_WAIT
    );

    // act
    readyToCommit(message1);
    readyToCommit(message2);
    readyToCommit(message3);
    onRebalance();

    // assert
    expect(consumer.commit).toHaveBeenCalledTimes(2);
    expect(consumer.commit).toHaveBeenNthCalledWith(1, [offsetDescription1]);
    expect(consumer.commit).toHaveBeenNthCalledWith(2, [
      offsetDescription2,
      offsetDescription3
    ]);
  });
});
