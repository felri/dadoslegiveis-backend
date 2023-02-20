from .db import execute_query
from db.db import connect_to_database


def get_latest_date():
    query = "SELECT MAX(datEmissao) FROM expenses;"
    result = execute_query(query, return_result=True)
    latest_date = result[0][0]
    return latest_date


def get_df_from_csv(path):
    import pandas as pd
    import glob

    all_files = glob.glob(path + "/*.csv")
    li = []
    for filename in all_files:
        df = pd.read_csv(filename, index_col=None, header=0)
        li.append(df)
    frame = pd.concat(li, axis=0, ignore_index=True)
    frame['sgUF'] = frame['sgUF'].fillna('')
    frame['sgPartido'] = frame['sgPartido'].fillna('')
    frame['txNomeParlamentar'] = frame['txNomeParlamentar'].str.replace("'", '')
    frame['txNomeParlamentar'] = frame['txNomeParlamentar'].str.replace('`', '')
    frame['txNomeParlamentar'] = frame['txNomeParlamentar'].str.replace('"', '')
    frame['txtCNPJCPF'] = frame['txtCNPJCPF'].str.replace('`', '')
    frame['txtCNPJCPF'] = frame['txtCNPJCPF'].str.replace("'", '')
    frame['txtCNPJCPF'] = frame['txtCNPJCPF'].str.replace('"', '')
    frame['txtNumero'] = frame['txtNumero'].str.replace("'", '')
    frame['txtNumero'] = frame['txtNumero'].str.replace('`', '')
    frame['txtNumero'] = frame['txtNumero'].str.replace('"', '')
    return frame


def save_to_db(df):
    conn = connect_to_database()
    cursor = conn.cursor()
    cursor.execute("BEGIN")
    for index, row in df.iterrows():
        print(row)
        query = f"""
        INSERT INTO expenses (
            txtNumero,
            vlrDocumento,
            datEmissao,
            txtCNPJCPF,
            txNomeParlamentar,
            numMes,
            numAno,
            numParcela,
            numLote,
            numRessarcimento,
            vlrRestituicao,
            nuDeputadoId,
            ideDocumento,
            urlDocumento,
            sgPartido,
            sgUF
        )
        VALUES (
            '{row['txtNumero']}',
            {row['vlrDocumento']},
            '{row['datEmissao']}',
            '{row['txtCNPJCPF']}',
            '{row['txNomeParlamentar']}',
            {row['numMes']},
            {row['numAno']},
            {row['numParcela']},
            {row['numLote']},
            {row['numRessarcimento']},
            {row['vlrRestituicao']},
            {row['nuDeputadoId']},
            {row['ideDocumento']},
            '{row['urlDocumento']}',
            '{row['sgPartido']}',
            '{row['sgUF']}'
        )
        ON CONFLICT DO NOTHING;
        """
        cursor.execute(query)
    
    cursor.execute("COMMIT")