import pandas as pd
import os
import duckdb
import pyarrow
from deltalake import DeltaTable, write_deltalake

dados = {
    'id': [1, 2, 3, 4, 5],
    'nome': ['Victor', 'Carlos', 'Lucas', 'Fernanda', 'Rafaela'],
    'idade': [26, 19, 20, 40, 32],
}

# Crio uma tabela Delta Lake para armazenar os dados no PC (em conjunto com Pandas)
df = pd.DataFrame(dados)
write_deltalake("tmp/some-table", df, mode="overwrite")



# Carrego a tabela Delta Lake para criar a Delta Table.
dt = DeltaTable("tmp/some-table")
print(f"Version: {dt.version()}")
print(f"Files: {dt.files()}")

# Conecto ao duck
conn = duckdb.connect(database=':memory:', read_only=False)
cursor = conn.cursor()

tabela = "CREATE TABLE dados (id INTEGER, nome VARCHAR, idade INTEGER);"
cursor.execute(tabela)
cursor.executemany("INSERT INTO dados VALUES (?, ?, ?)", df.values.tolist())
conn.commit()

# Busca
cursor.execute("SELECT * FROM dados;")
select = cursor.fetchall()  # trago pro programa
print(select)

conn.close()
