import pikabu_parser_basic as base
import argparse
import time
from joblib import Parallel, delayed


blocks = 8


local_list_data = [None]*blocks
local_list_error = [None]*blocks


def updated_parser(i: int, l:int, r:int):
    data, error_list = base.get_article_range(l, r)
    local_list_data[i] = data
    local_list_error[i] = error_list


def parallel_parser(l:int, r:int):
    step = int((r-l)/blocks) + 1

    with Parallel(n_jobs=8, require='sharedmem') as parallel:
        parallel(delayed(updated_parser)(i, val, min(val+step, r)) for i, val in enumerate(range(l, r, step)))

    res_data = local_list_data[0]
    for elem in local_list_data[1:]:
        if elem is not None:
            res_data = res_data.append(elem)

    error_list = list()
    for elem in local_list_error:
        if elem:
            error_list += elem

    return res_data, error_list


if __name__ == '__main__':
    base.may_be_main(parallel_parser)
