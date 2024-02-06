import pandas as pd
import os
import duckdb
import pyarrow

dados = {
    'id': [1, 2, 3, 4, 5],
    'nome': ['Victor', 'Carlos', 'Lucas', 'Fernanda', 'Rafaela'],
    'idade': [26, 19, 20, 40, 32],
}

df = pd.DataFrame(dados) # Tabela Delta

delta_path = 'csv'
if not os.path.exists(delta_path):
    os.makedirs(delta_path)

# Salvo os dados em um csv pra ver se tão ok
csv_file_path = os.path.join(delta_path, 'data.csv')
df.to_csv(csv_file_path, index=False)

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
