import pandas as pd
import ast


if __name__ == '__main__':
    data = pd.read_csv('data0_1000.csv')

    print(data['tags'])
    for elem in data['tags']:
        print(elem, type(elem))
        ttt = ast.literal_eval(elem)
        print(ttt, type(ttt))
        for rrr in ttt:
            print(rrr)
    print(type(data['tags']))
