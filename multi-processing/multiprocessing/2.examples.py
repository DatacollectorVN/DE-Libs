import multiprocessing as mp
import time
import math

def calculate_one(numbers):
    result = []
    for number in numbers:
        result.append(math.sqrt(number ** 3))

def calculate_two(numbers):
    result = []
    for number in numbers:
        result.append(math.sqrt(number ** 4))

def calculate_three(numbers):
    result = []
    for number in numbers:
        result.append(math.sqrt(number ** 5))

def run_without_multi():
    numbers = list(range(10000000))
    start_time = time.time()
    calculate_one(numbers)
    calculate_two(numbers)
    calculate_three(numbers)
    end_time = time.time()
    duration = end_time - start_time
    print(f'Duration without multiprocessing: {duration}')

def run_with_multi():
    numbers = list(range(10000000))
    p1 = mp.Process(target = calculate_one, args=(numbers,))
    p2 = mp.Process(target = calculate_two, args=(numbers,))
    p3 = mp.Process(target = calculate_three, args=(numbers,))
    
    # start the processors
    start_time = time.time()
    p1.start()
    p2.start()
    p3.start()
    end_time = time.time()
    duration = end_time - start_time
    print(f'Duration with multiprocessing: {duration}')
    
    # close the processors
    p1.join()
    p2.join()
    p3.join()

if __name__ == '__main__':
    run_without_multi()
    run_with_multi()