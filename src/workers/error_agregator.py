import os


def error_agregator(reads, write):
    data = list()
    unique_errors = list()
    for read in reads:
        with open(read, 'r') as f_in:
            for line in f_in:
                num, mes = line.split(' ', 1)
                try:
                    num = int(num)
                except ValueError:
                    print(f'error with id {num}')
                    raise
                data.append((num, mes))
                if mes != 'redirected to main page\n':
                    print(num, mes)
                if mes not in unique_errors:
                    unique_errors.append(mes)
    print(unique_errors)
    with open(write, 'w+') as f_out:
        for num, mes in data:
            f_out.write(f"{num} {mes}")

    for read in reads:
        os.remove(read)


if __name__ == '__main__':
    start = 180000
    end   = 190000
    step  = 1000

    ids = list(range(start, end+1, step))
    names = [f'log_mes/log_{it1}_{it2}.txt' for it1, it2 in zip(ids[:-1], ids[1:])]
    # print(names)

    error_agregator(names, f'log_mes/log_{start}_{end}.txt')
