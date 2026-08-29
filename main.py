import numpy as np
import pandas as pd

def main():
    print("Hello from dic!")
    arr = np.array([1, 2, 3])
    print("Numpy array:", arr)
    df = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
    print("Pandas DataFrame:\n", df)


if __name__ == "__main__":
    main()
