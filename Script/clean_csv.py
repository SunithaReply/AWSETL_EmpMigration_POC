import pandas as pd
pd.set_option('display.max_columns',10)

df = pd.read_csv("50_employees_copy.csv")

#df = df[:1000]

df = df.drop("num",axis = 1)

address = df["address"]

Address = []

for row in address:

    row = row.replace("\n", " ")
    row = row.replace(",", " ")

    Address.append(row)

df["address"] = Address
print(df)
df.to_csv("50-employees.csv", index = False)