import { useCommitManager } from ".";
import { KafkaConsumer } from "node-rdkafka";

jest.mock("node-rdkafka");

describe("useCommitManager", () => {
  const ONE_MILLISECOND = 1;
  const TWO_MILLISECONDS = 2;
  const offsetDescription1 = { topic: "abc", partition: 1, offset: 5 };
  const message1 = { ...offsetDescription1, value: "test1" };
  const offsetDescription2 = { topic: "def", partition: 2, offset: 2 };
  const message2 = { ...offsetDescription2, value: "test2" };
  const offsetDescription3 = { topic: "ghi", partition: 3, offset: 4 };
  const message3 = { ...offsetDescription3, value: "test3" };

  it("only commits the first offset immediately", () => {
    // arrange
    const commit = jest.fn();
    const consumer = new KafkaConsumer({}, {});
    consumer.commit = commit;
    const { readyToCommit } = useCommitManager(consumer, ONE_MILLISECOND);

    // act
    readyToCommit(message1);
    readyToCommit(message2);
    readyToCommit(message3);

    // assert
    expect(consumer.commit).toHaveBeenCalledTimes(1);
    expect(consumer.commit).toHaveBeenCalledWith([offsetDescription1]);
  });

  it("commits the second offset immediately if the commit interval has already been exceeded", async () => {
    // arrange
    const commit = jest.fn();
    const consumer = new KafkaConsumer({}, {});
    consumer.commit = commit;
    const { readyToCommit } = useCommitManager(consumer, ONE_MILLISECOND);

    // act
    readyToCommit(message1);
    await new Promise(resolve => setTimeout(resolve, ONE_MILLISECOND + 1));
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
    const { readyToCommit } = useCommitManager(consumer, ONE_MILLISECOND);

    // act
    readyToCommit(message1);
    readyToCommit(message2);
    readyToCommit(message3);
    await new Promise(resolve => setTimeout(resolve, ONE_MILLISECOND + 1));

    // assert
    expect(consumer.commit).toHaveBeenCalledTimes(2);
    expect(consumer.commit).toHaveBeenNthCalledWith(1, [offsetDescription1]);
    expect(consumer.commit).toHaveBeenCalledWith([
      offsetDescription2,
      offsetDescription3
    ]);
  });

  it("does not immediately commit the third message if the commit interval was exceeded between the second and third messages", async () => {
    // arrange
    const commit = jest.fn();
    const consumer = new KafkaConsumer({}, {});
    consumer.commit = commit;
    const { readyToCommit } = useCommitManager(consumer, TWO_MILLISECONDS);

    // act
    readyToCommit(message1);
    readyToCommit(message2);
    await new Promise(resolve => setTimeout(resolve, TWO_MILLISECONDS + 1));
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
    const { readyToCommit } = useCommitManager(consumer, TWO_MILLISECONDS);

    // act
    readyToCommit(message1);
    readyToCommit(message2);
    await new Promise(resolve => setTimeout(resolve, TWO_MILLISECONDS + 1));
    readyToCommit(message3);
    await new Promise(resolve => setTimeout(resolve, TWO_MILLISECONDS));

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
      ONE_MILLISECOND
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
