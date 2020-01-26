import pandas as pd
import os


if __name__ == '__main__':
    read1 = 'data2/data_3000_3800.csv'
    read2 = 'data2/data_3800_4000.csv'
    write = 'data2/data_3000_4000.csv'

    df1 = pd.read_csv(read1, index_col=0)
    df2 = pd.read_csv(read2, index_col=0)
    df2 = df1.append(df2)

    df2.to_csv(write)

    os.remove(read1)
    os.remove(read2)
