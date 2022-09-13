import asyncio

async def main_1():
    print('Hello Nathan')

async def main_2():
    print('Hello Nathan')
    task = asyncio.create_task(sub_main_1(job = 'data engineer'))
    print('Done')

async def main_3():
    print('Hello Nathan')
    task = asyncio.create_task(sub_main_1(job = 'data engineer'))
    await task # waiting until this `task` is finish
    print('Done')

async def sub_main_1(job):
    print(f'my job is {job}')
    await asyncio.sleep(2) # sleep 2 seconds

if __name__ == '__main__':
    def run_1():
        try:
            main_1() 
        except RuntimeWarning as e:
            print(e)
    # run_1() # get error: coroutine 'main_1' was never awaited

    def run_2():
        try:
            asyncio.run(main_1()) 
        except RuntimeWarning as e:
            print(e)
    # run_2() # expected result: Hello Nathan

    def run_3():
        try:
            asyncio.run(main_2())
        except RuntimeWarning as e:
            print(e)
    # run_3()
    ''' expected result:
    >> Hello Nathan
    >> Done
    >> my job is data engineer
    '''

    def run_4():
        try:
            asyncio.run(main_3())
        except RuntimeWarning as e:
            print(e)
    run_4()
    ''' expected result:
    >> Hello Nathan
    >> my job is data engineer
    >> Done
    '''
