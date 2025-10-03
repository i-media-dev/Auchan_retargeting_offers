import pandas as pd

df = pd.DataFrame({
    'a': ['a1', 'a2'],
    'b': ['b1', 'b2']
})

def pair(e):
    e['c'] = 1
    print(e)
    return e

df = df.apply(lambda x: pair(x), axis=1)

print()
print(df)