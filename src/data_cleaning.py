from math import ceil
import pandas as pd
import os

def batch_save_to_csv(path, df: pd.DataFrame, debug=False):
    frags = int(df.memory_usage(deep=True).sum() / (1024 ** 2) / 100) + 1

    if frags == 1:
        df.to_csv(path + ".csv")
        return

    batch_size = ceil(len(df) / frags)

    if debug:
        print(f"{frags=}, {batch_size=}")

    for i in range(frags):
        start = i * batch_size
        end = start + batch_size

        if debug:
            print(f"df[{start}:{end}]")

        df_cut = df.iloc[start:end]
        df_cut.to_csv(f"{path}_{i}.csv")

def batch_load_from_csv(path, debug=False) -> pd.DataFrame:
    dir = path[:path.rfind("/")+1]
    
    if debug:
        print(f"Entering {dir}")

    num_files = len(os.listdir(dir))
    dfs = []

    for i in range(num_files):
        fp = f"{path}_{i}.csv"
        if debug:
            print(f"Loading from {fp}")
        dfs.append(pd.read_csv(fp))


    print("Concatting All...")
    ret = pd.concat(dfs)

    return ret