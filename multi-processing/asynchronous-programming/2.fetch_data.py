import asyncio
import random

async def fetch_data():
    print('Start fetching')
    await asyncio.sleep(2)
    value = random.randint(0, 10)
    print('Done fetching')
    return {'data': value}

async def input_data(value):
    print('Start prining')
    for i in range(value):
        print(i)
        await asyncio.sleep(0.5)

async def main_1():
        task_1 = asyncio.create_task(fetch_data())
        value =  task_1
        print(value)

async def main_2():
        task_1 = asyncio.create_task(fetch_data())
        value =  await task_1
        print(value)

async def main_3():
        task_1 = asyncio.create_task(fetch_data())
        task_2 = asyncio.create_task(input_data(10))
        value =  await task_1
        print(value)
        await task_2

if __name__ == '__main__':
    def run_1():
        try:
            asyncio.run(main_1())
        except RuntimeWarning as e:
            print(e)
    # run_1()

    def run_2():
        try:
            asyncio.run(main_2())
        except RuntimeWarning as e:
            print(e)
    # run_2()

    def run_3():
        try:
            asyncio.run(main_3())
        except RuntimeWarning as e:
            print(e)
    # run_3()