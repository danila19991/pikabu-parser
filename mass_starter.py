from subprocess import Popen
import time


if __name__ == '__main__':
    start_id = 6000
    step = 1000
    while True:
        start = time.time()
        p = Popen(['python', 'pikabu_parser_basic.py', str(start_id), str(start_id + step)])
        p.communicate()
        end = time.time()
        print(start_id, end-start)
        start_id += step
