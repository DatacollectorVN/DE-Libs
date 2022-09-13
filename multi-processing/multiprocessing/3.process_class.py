import multiprocessing as mp
import math
from typing import List

numbers = [2, 7, 5, 4, 3]

def calculate_mp_1(numbers: List, result_list):
    for number in numbers:
        result_list.append(math.sqrt(number ** 3))    

def calculate_mp_2(numbers, result_dict):
    for i, number in enumerate(numbers):
        result_dict[i] = math.sqrt(number ** 3)

if __name__ == "__main__":
    manager = mp.Manager()
    result_list = manager.list()
    result_dict = manager.dict()

    process_1 = mp.Process(target = calculate_mp_1, args = (numbers, result_list))
    process_2 = mp.Process(target = calculate_mp_2, args = (numbers, result_dict))
    process_1.start()
    process_2.start()

    process_1.join()
    process_2.join()

    print(result_list)
    print(result_dict.values())
