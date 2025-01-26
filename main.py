import asyncio
import time
from random import randint

"""
Simulating the Producer-Consumer problem, where a producer publishes messages faster than a consumer can process them.

Key Challenges Addressed:
1. **Event Loop Blocking**: The GIL causes the event loop to block during CPU-intensive tasks in threads.
2. **Queue Backpressure Issues**: If the consumer is slow, the queue might fill up, causing the producer to block. This introduces the concept of `backpressure` where the producer is implicitly forced to slow down.
3. **Difficulty in Scaling**: Adding more consumers (threads) doesn't improve throughput for CPU-bound tasks, as they all contend for the GIL.
"""

queue = asyncio.Queue(maxsize=5)


async def publisher():
    """
    Asynchronous producer function that generates random integers and adds them to a shared queue.

    - The producer runs indefinitely, inserting random values into the queue every second.
    - Implements a delay (`await asyncio.sleep(1)`) to simulate a realistic production interval.
    """
    while True:
        value: int = randint(0, 1000)
        await queue.put(value)
        print(f"Inserted {value} in queue")
        await asyncio.sleep(.5)


async def consumer():
    """
    Asynchronous consumer function that retrieves integers from the shared queue and processes them.

    - Consumes values from the queue.
    - Offloads CPU-intensive tasks to a separate thread using `asyncio.to_thread`, preventing event loop blocking.
    """
    while True:
        value: int = await queue.get()
        print(f"Consumed {value} from queue")
        await asyncio.to_thread(cpu_intensive_task, value)


def cpu_intensive_task(value: int):
    """
    Simulates a CPU-intensive computation for a given value.

    - Uses a busy-wait loop to mimic a computation-heavy task lasting approximately 2 seconds.
    - Designed to run in a separate thread to avoid blocking the event loop.

    Args:
        value (int): The value being processed by the CPU-intensive task.
    """
    print(f"Starting CPU-intensive task for {value}")
    start = time.time()
    while time.time() - start < 3:  # Simulated CPU work for 2 seconds
        ...
    print(f"Finished CPU-intensive task for {value}")


async def main():
    try:
        await asyncio.gather(
            publisher(),
            consumer()
        )
    except asyncio.CancelledError:
        print("Shutting down gracefully...")


# Run the main event loop
asyncio.run(main())
