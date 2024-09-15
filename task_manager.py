import asyncio
import concurrent.futures
import time
import random
import psutil

class CustomException(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)

class TaskManager:
    PROCESS=1
    THREAD=0
    def __init__(self,mode=0, workers=None):
        self.mode = mode
        self.loop = asyncio.get_event_loop()
        if mode:
            self.executor = concurrent.futures.ProcessPoolExecutor(workers) 
        else:
            self.executor = concurrent.futures.ThreadPoolExecutor(workers)
        self.tasks = []

    async def add_io_task(self, coro):
        task = self.loop.create_task(coro)
        self.tasks.append(task)
        return task

    async def add_cpu_task(self, func, *args):
        task = self.loop.run_in_executor(self.executor, func, *args)
        self.tasks.append(task)
        return task

    async def get_results_as_ready(self):
        for task in asyncio.as_completed(self.tasks):
            try:
                result = await task
            except CustomException as e:
                result = e.value
            yield result
        self.tasks = []  # Clear the tasks list after gathering results

    async def run_forever(self):
        while True:
            await asyncio.sleep(1)  # Keep the loop running

# Example IO-bound task
async def io_task(idx: int):
    delay = 0.2 
    await asyncio.sleep(delay)
    eventuality = round(random.uniform(0.2,1.0),2)
    if eventuality > 0.75:
        raise CustomException(f"IO-bound task error <{idx}>")
    result = f"IO-bound task completed <{idx}> cpu: {psutil.Process().cpu_num()}"
    return result

# Example CPU-bound task
def cpu_task(idx: int):
    result = 0
    for i in range(random.randint(100_000, 500_000)):
        result += i
    eventuality = round(random.uniform(0.2,1.0),2)
    if eventuality > 0.75:
        raise CustomException(f"CPU-bound task error <{idx}>")
    result = f"CPU-bound task completed <{idx}> cpu: {psutil.Process().cpu_num()}"
    return result

async def runtime_task_creator(manager, end_after_seconds=60):
    start = time.monotonic()
    while (time.monotonic()-start < end_after_seconds):
        await manager.add_io_task(io_task(9999))
        await asyncio.sleep(0.2)

async def main():
    manager = TaskManager(workers=None,mode=TaskManager.PROCESS)

    # Add tasks
    for idx in range(500):
        await manager.add_io_task(io_task(idx))
        await manager.add_cpu_task(cpu_task,idx)

    #await runtime_task_creator(manager,end_after_seconds=30)

    # Run the task manager in the background
    start = time.monotonic()
    asyncio.create_task(manager.run_forever())

    # Get results from tasks as they are ready
    async for result in manager.get_results_as_ready():
        print(result)

    print(f'All task completed in {time.monotonic()-start:.2f}')

if __name__ == "__main__":
    asyncio.run(main())
