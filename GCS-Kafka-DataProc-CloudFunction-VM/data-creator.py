# generate_data.py
import pandas as pd
import numpy as np

df = pd.DataFrame({
    "id": range(1000),
    "value": np.random.randint(1, 100, 1000)
})

df.to_csv("data1.csv", index=False)
df.to_csv("data2.csv", index=False)