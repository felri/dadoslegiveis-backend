import pandas as pd
from . import db


def create_table():
    # Check if the table already exists
    query = """
        SELECT EXISTS (SELECT 1 
        FROM information_schema.tables 
        WHERE table_name = 'deputados');
    """
    result = db.execute_query(query, return_result=True)
    if result[0][0]:
        print("Table already exists, skipping creation")
        return

    query = """
        CREATE TABLE deputados (
            uri VARCHAR(255) NOT NULL,
            nome VARCHAR(255) NOT NULL UNIQUE,
            idLegislaturaInicial INTEGER,
            idLegislaturaFinal INTEGER,
            nomeCivil VARCHAR(255),
            cpf VARCHAR(255),
            siglaSexo CHAR(1),
            urlRedeSocial VARCHAR(255),
            urlWebsite VARCHAR(255),
            dataNascimento DATE,
            ufNascimento CHAR(20),
            municipioNascimento VARCHAR(255)
        );
    """
    db.execute_query(query)


def insert_data(row):
    query = """
        INSERT INTO deputados (uri, nome, idLegislaturaInicial, 
        idLegislaturaFinal, nomeCivil, cpf, siglaSexo, urlRedeSocial,
        urlWebsite, dataNascimento, ufNascimento, 
        municipioNascimento)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
    """

    # Replace empty strings with NULL
    params = []
    for value in row:
        if value == "":
            params.append(None)
        else:
            params.append(value)
    db.execute_query(query, params)


def load_data(csv_file):
    create_table()
    # Load the CSV file into a DataFrame
    df = pd.read_csv(csv_file, sep=";")

    df = df[df["dataFalecimento"].isnull()]
    df = df[df["dataNascimento"].notnull()]
    df = df[df["ufNascimento"].notnull()]
    df = df[df["municipioNascimento"].notnull()]
    df["dataNascimento"] = pd.to_datetime(df["dataNascimento"], errors="coerce")
    df.drop(columns=["dataFalecimento"], inplace=True)

    # Remove rows where idLegislaturaInicial is less than 40
    df = df[df.idLegislaturaInicial >= 40]

    # Remove duplicates based on the nome column, keeping the first
    df["nome"] = df["nome"].apply(db.remove_accents)
    df.drop_duplicates(subset="nome", keep=False, inplace=True)

    # Iterate over the rows of the DataFrame and insert the cleaned data
    for _, row in df.iterrows():
        print(row.values)
        insert_data(row.values)
