import pikabu_parser_basic as base
from multiprocessing import Process, Queue
import argparse
import time


blocks = 8


def updated_parser(q, l:int, r:int):
    data, error_list = base.get_article_range(l, r)
    q.put((data, error_list))


def parallel_parser(l:int, r:int):
    step = int((r-l)/blocks) + 1
    ths = list()
    q = Queue()

    for i, val in enumerate(range(l, r, step)):
        th = Process(target=updated_parser, args=(q, val, min(val+step, r)))
        th.start()
        ths.append(th)

    res_data = None
    error_list = list()

    for _ in enumerate(ths):
        data, error = q.get()
        if res_data is None:
            res_data = data
        else:
            res_data = res_data.append(data)

        if error:
            error_list += error

    for th in ths:
        th.join()

    return res_data, error_list


if __name__ == '__main__':
    base.may_be_main(parallel_parser)
