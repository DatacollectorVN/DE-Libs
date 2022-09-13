# https://www.pythontutorial.net/python-concurrency/python-asyncio-gather/
import asyncio
import random


class APIError(Exception):
    def __init__(self, message):
        self._message = message

    def __str__(self):
        return self._message

async def call_api(message, delay=2):
    print(message)
    await asyncio.sleep(delay)
    result = random.randint(0, 10)
    return result

async def call_api_failed():
    await asyncio.sleep(3)
    raise APIError('API failed')
    
async def main_1():
    call_apis = [call_api('Calling API 1 ...', 1), call_api('Calling API 2 ...', 2)]
    results = await asyncio.gather(
        *call_apis
    )
    print(results)

async def main_2():
    call_apis = [call_api('Calling API 1 ...', 1), call_api('Calling API 2 ...', 2), call_api_failed()]
    results = await asyncio.gather(
        *call_apis
    )
    print(results)

async def main_3():
    call_apis = [call_api('Calling API 1 ...', 1), call_api('Calling API 2 ...', 2), call_api_failed()]
    results = await asyncio.gather(
        *call_apis, return_exceptions = True
    )
    print(results)

if __name__ == '__main__':
    def run_1():
        try:
            asyncio.run(main_1())
        except Exception as e:
            print(e)
    # run_1()
    
    def run_2():
        try:
            asyncio.run(main_2())
        except Exception as e:
            print(e)
    # run_2()

    def run_3():
        try:
            asyncio.run(main_3())
        except Exception as e:
            print(e)
    run_3()

