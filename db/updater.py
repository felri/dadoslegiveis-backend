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
    frame["txNomeParlamentar"] = frame["txNomeParlamentar"].str.replace("'", "")
    frame["txNomeParlamentar"] = frame["txNomeParlamentar"].str.replace("`", "")
    frame["txNomeParlamentar"] = frame["txNomeParlamentar"].str.replace('"', "")
    frame["txtFornecedor"] = frame["txtFornecedor"].str.replace("'", "")
    frame["txtFornecedor"] = frame["txtFornecedor"].str.replace("`", "")
    frame["txtFornecedor"] = frame["txtFornecedor"].str.replace('"', "")
    frame["txtCNPJCPF"] = frame["txtCNPJCPF"].str.replace("`", "")
    frame["txtCNPJCPF"] = frame["txtCNPJCPF"].str.replace("'", "")
    frame["txtCNPJCPF"] = frame["txtCNPJCPF"].str.replace('"', "")
    frame["txtNumero"] = frame["txtNumero"].str.replace("'", "")
    frame["txtNumero"] = frame["txtNumero"].str.replace("`", "")
    frame["txtNumero"] = frame["txtNumero"].str.replace('"', "")
    frame = frame.fillna("")

    return frame


def save_to_db(df):
    conn = connect_to_database()
    cursor = conn.cursor()
    print("beginning transaction")
    cursor.execute("BEGIN")
    for index, row in df.iterrows():
        query = f"""
        INSERT INTO expenses (
            txNomeParlamentar,
            cpf,
            ideCadastro,
            nuLegislatura,
            sgUF,
            sgPartido,
            codLegislatura,
            numSubCota,
            txtDescricao,
            txtFornecedor,
            txtCNPJCPF,
            txtNumero,
            datEmissao,
            vlrDocumento,
            vlrLiquido,
            numMes,
            numAno,
            txtPassageiro,
            txtTrecho,
            urlDocumento
        )
        VALUES (
            '{row["txNomeParlamentar"]}',
            '{row["cpf"]}',
            '{row["ideCadastro"]}',
            '{row["nuLegislatura"]}',
            '{row["sgUF"]}',
            '{row["sgPartido"]}',
            '{row["codLegislatura"]}',
            '{row["numSubCota"]}',
            '{row["txtDescricao"]}',
            '{row["txtFornecedor"]}',
            '{row["txtCNPJCPF"]}',
            '{row["txtNumero"]}',
            '{row["datEmissao"]}',
            '{row["vlrDocumento"]}',
            '{row["vlrLiquido"]}',
            '{row["numMes"]}',
            '{row["numAno"]}',
            '{row["txtPassageiro"]}',
            '{row["txtTrecho"]}',
            '{row["urlDocumento"]}'
        )
        ON CONFLICT DO NOTHING;
        """
        cursor.execute(query)

    cursor.execute("COMMIT")
