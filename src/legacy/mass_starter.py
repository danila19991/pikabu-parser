from subprocess import Popen, DEVNULL
import time


if __name__ == '__main__':
    start_id = 5000
    step = 1000
    g_len = 1
    while True:
        start = time.time()
        p_list = list()
        for i in range(g_len):
            p = Popen(['python', 'pikabu_parser_basic.py', str(start_id + (i)*step), str(start_id + (i+1)*step)],
                      stdout=DEVNULL, stderr=DEVNULL)

            p_list.append(p)
        for p in p_list:
            p.communicate()
        end = time.time()
        print(start_id, end-start)
        start_id += step*g_len
        # break
