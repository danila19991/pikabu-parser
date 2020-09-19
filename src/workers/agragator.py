import pandas as pd
import os


def agreg_func(reads, write):
    data = list()
    for read in reads:
        data.append(pd.read_csv(read, index_col=0))

    res = data[0]
    for d in data[1:]:
        res.append(d)

    res.to_csv(write)

    for read in reads:
        os.remove(read)


if __name__ == '__main__':
    start = 180000
    end   = 190000
    step  = 1000

    ids = list(range(start, end+1, step))
    names = [f'data2/data_{it1}_{it2}.csv' for it1, it2 in zip(ids[:-1], ids[1:])]
    # print(names)

    agreg_func(names, f'data2/data_{start}_{end}.csv')
