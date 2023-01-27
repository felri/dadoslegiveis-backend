import csv
import pandas as pd
from .db import execute_query
import glob
from scripts.cache import cache_function


def create_table():
    # Check if the table already exists
    query = """
        SELECT EXISTS (SELECT 1 
        FROM information_schema.tables 
        WHERE table_name = 'expenses');
    """
    result = execute_query(query, return_result=True)
    if result[0][0]:
        print("Table already exists, skipping creation")
        return

    query = """
        CREATE TABLE expenses (
            txNomeParlamentar VARCHAR(255),
            cpf VARCHAR(255),
            ideCadastro INTEGER,
            nuCarteiraParlamentar INTEGER,
            nuLegislatura INTEGER,
            sgUF CHAR(2),
            sgPartido VARCHAR(20),
            codLegislatura INTEGER,
            numSubCota INTEGER,
            txtDescricao VARCHAR(255),
            numEspecificacaoSubCota INTEGER,
            txtDescricaoEspecificacao VARCHAR(255),
            txtFornecedor VARCHAR(255),
            txtCNPJCPF VARCHAR(255),
            txtNumero VARCHAR(255),
            indTipoDocumento CHAR(100),
            datEmissao DATE,
            vlrDocumento NUMERIC,
            vlrGlosa NUMERIC,
            vlrLiquido NUMERIC,
            numMes INTEGER,
            numAno INTEGER,
            numParcela INTEGER,
            txtPassageiro VARCHAR(255),
            txtTrecho VARCHAR(255),
            numLote VARCHAR(255),
            numRessarcimento VARCHAR(255),
            vlrRestituicao NUMERIC,
            nuDeputadoId INTEGER,
            ideDocumento INTEGER,
            urlDocumento VARCHAR(255)
        );

        CREATE UNIQUE INDEX expenses_unique_index ON expenses (
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
            urlDocumento
        );

        CREATE INDEX datEmissao_index ON expenses (datEmissao);
        CREATE INDEX txtDescricao_index ON expenses (txtDescricao);
        CREATE INDEX txNomeParlamentar_index ON expenses (txNomeParlamentar);
    """
    execute_query(query)


def read_csv(file_path):
    with open(file_path, "r", encoding="utf-8-sig") as file:
        # Use the csv.reader function to read the file
        reader = csv.reader(file, delimiter=";", quotechar='"')
        # Get the column names from the first line of the file
        column_names = next(reader)
        # Initialize an empty list to store the records
        rows = []
        # Iterate through the remaining lines of the file
        for row in reader:
            # Zip the column names and row values together to create a dictionary
            # for each record
            rows.append(dict(zip(column_names, row)))
    return rows


def read_all_csvs(directory):
    file_pattern = f"{directory}/*.csv"
    file_paths = glob.glob(file_pattern)
    rows = []
    for file_path in file_paths:
        csv = read_csv(file_path)
        rows.append(csv)
    return rows


def trim_sgpartido(row):
    if row["sgPartido"] is not None:
        row["sgPartido"] = row["sgPartido"].strip()
    return row


def handle_empty_date_fields(row):
    if row["datEmissao"] == "":
        row["datEmissao"] = None
    return row


def handle_empty_float_fields(row):
    float_fields = [
        "vlrRestituicao",
        "vlrRestituicao",
        "vlrGlosa",
        "vlrLiquido",
    ]
    for field in float_fields:
        if row[field] == "":
            row[field] = None
        elif row[field] is not None:
            try:
                row[field] = float(row[field])
            except ValueError:
                # log the error and set the field to None
                print(f"Unable to convert {row[field]} to float for field {field}")
                row[field] = None
    return row


def handle_empty_integer_fields(row):
    integer_fields = [
        "ideCadastro",
        "nuCarteiraParlamentar",
        "nuLegislatura",
        "codLegislatura",
        "numSubCota",
        "numEspecificacaoSubCota",
        "numMes",
        "numAno",
        "numParcela",
        "numLote",
        "numRessarcimento",
        "nuDeputadoId",
        "ideDocumento",
    ]
    for field in integer_fields:
        if row[field] == "":
            row[field] = None
        elif row[field] is not None:
            try:
                row[field] = int(row[field])
            except ValueError:
                # log the error and set the field to None
                print(f"Unable to convert {row[field]} to integer for field {field}")
                row[field] = None
    return row


def get_treemap_data(start_date, end_date):
    cache_key = f"treemap_data_{start_date}_{end_date}"

    def data_function():
        query = """
            SELECT 
                txtDescricao, SUM(vlrLiquido) as expense, COUNT(txtDescricao) as count
            FROM
                expenses
            WHERE datEmissao::date BETWEEN %s AND %s
            GROUP BY
                txtDescricao
            ORDER BY SUM(vlrLiquido) DESC 
            LIMIT 7
        """
        results = execute_query(query, (start_date, end_date), return_result=True)
        return results
    
    return cache_function(data_function, cache_key)


def get_barplot_treemap_block_data(description, start_date, end_date):
    cache_key = f"barplot_treemap_block_data_{description}_{start_date}_{end_date}"

    def data_function():
        query = """
            SELECT txNomeParlamentar, SUM(vlrDocumento)
            FROM expenses
            WHERE txtDescricao LIKE %s
            AND datEmissao::date BETWEEN %s AND %s
            GROUP BY
                txNomeParlamentar
            ORDER BY SUM(vlrLiquido) ASC 
        """
        results = execute_query(
            query, (description, start_date, end_date), return_result=True
        )
        return results
    
    return cache_function(data_function, cache_key)


def get_details_by_name_and_day(day, name, by_party=False):
    if by_party:
        query = """
            SELECT 
                expenses.*,
                deputados.*
            FROM expenses
            LEFT JOIN deputados ON expenses.txNomeParlamentar = deputados.nome
            WHERE datEmissao = %s
            AND sgPartido = %s
        """
    else:
        query = """
            SELECT 
                expenses.*,
                deputados.*
            FROM expenses
            LEFT JOIN deputados ON expenses.txNomeParlamentar = deputados.nome
            WHERE datEmissao = %s
            AND txNomeParlamentar = %s
        """
    params = (day, name)
    result = execute_query(query, params, return_result=True)

    keys = [
        "txNomeParlamentar",
        "cpf",
        "ideCadastro",
        "nuCarteiraParlamentar",
        "nuLegislatura",
        "sgUF",
        "sgPartido",
        "codLegislatura",
        "numSubCota",
        "txtDescricao",
        "numEspecificacaoSubCota",
        "txtDescricaoEspecificacao",
        "txtFornecedor",
        "txtCNPJCPF",
        "txtNumero",
        "indTipoDocumento",
        "datEmissao",
        "vlrDocumento",
        "vlrGlosa",
        "vlrLiquido",
        "numMes",
        "numAno",
        "numParcela",
        "txtPassageiro",
        "txtTrecho",
        "numLote",
        "numRessarcimento",
        "vlrRestituicao",
        "nuDeputadoId",
        "ideDocumento",
        "urlDocumento",
        "uri",
        "nome",
        "idLegislaturaInicial",
        "idLegislaturaFinal",
        "nomeCivil",
        "cpf",
        "siglaSexo",
        "urlRedeSocial",
        "urlWebsite",
        "dataNascimento",
        "ufNascimento",
        "municipioNascimento",
    ]

    results = [dict(zip(keys, row)) for row in result]
    return results


def get_query_result_by_deputy(start_date, end_date):
    query = """
        SELECT 
            txNomeParlamentar, 
            datEmissao, 
            sum(vlrDocumento) as vlrDocumento
        FROM expenses
        WHERE datEmissao BETWEEN %s AND %s
        GROUP BY txNomeParlamentar, datEmissao
        ORDER BY datEmissao ASC
    """
    params = (start_date, end_date)
    result = execute_query(query, params, return_result=True)
    return result


def get_query_result_by_party(start_date, end_date):
    query = """
        SELECT 
            TRIM(sgPartido), 
            datEmissao, 
            sum(vlrDocumento) as vlrDocumento
        FROM expenses
        WHERE datEmissao BETWEEN %s AND %s
        GROUP BY sgPartido, datEmissao
        ORDER BY datEmissao ASC
    """
    params = (start_date, end_date)
    result = execute_query(query, params, return_result=True)
    return result


def get_pivoted_dataframe(result, selector):
    df = pd.DataFrame(result, columns=[selector, "datEmissao", "vlrDocumento"])
    pivoted_df = df.pivot_table(
        index=selector,
        columns="datEmissao",
        values="vlrDocumento",
        aggfunc="sum",
    )
    pivoted_df = pivoted_df.fillna(0)
    pivoted_df = pivoted_df[(pivoted_df.T != 0).any()]
    return pivoted_df


def get_total_expenses(pivoted_df):
    pivoted_df["total_expenses"] = pivoted_df.sum(axis=1)
    pivoted_df = pivoted_df.sort_values(by="total_expenses", ascending=False)
    return pivoted_df


def get_dates_and_series(pivoted_df):
    # drop the total expenses column
    pivoted_df = pivoted_df.drop("total_expenses", axis=1)
    # Get the dates from the columns of the pivoted dataframe
    dates = pivoted_df.columns.tolist()
    dates = dates
    # Initialize the list of series
    series = []
    # Iterate over the rows of the pivoted dataframe
    for row in pivoted_df.iterrows():
        name = row[0]
        values = row[1].tolist()
        series.append({"name": name, "values": values})
    return dates, series


def get_joyplot_data(start_date, end_date, by_party=False):
    """
    A function that returns joyplot data
    """
    # Generate the cache key
    cache_key = f"joyplot_data:{start_date}:{end_date}:{by_party}"

    # Define the function to be called if the data is not in the cache
    def data_function():
        selector = "sgPartido" if by_party else "txNomeParlamentar"
        if by_party:
            result = get_query_result_by_party(start_date, end_date)
        else:
            result = get_query_result_by_deputy(start_date, end_date)

        pivoted_df = get_pivoted_dataframe(result, selector)
        pivoted_df = get_total_expenses(pivoted_df)
        dates, series = get_dates_and_series(pivoted_df)
        data = {
            "dates": [date.strftime('%Y-%m-%d') for date in dates],
            "series": series[:513],
            "total": len(series),
        }
        return data

    # Call the cache function
    return cache_function(data_function, cache_key)


def get_list_expenses_by_deputy(description, startDate, endDate, name):
    cache_key = f"list_expenses_by_deputy:{description}:{startDate}:{endDate}:{name}"

    def data_function():
        query = """
            SELECT 
                expenses.txNomeParlamentar,
                expenses.cpf,
                expenses.ideCadastro,
                expenses.nuCarteiraParlamentar,
                expenses.nuLegislatura,
                expenses.sgUF,
                expenses.sgPartido,
                expenses.codLegislatura,
                expenses.numSubCota,
                expenses.txtDescricao,
                expenses.numEspecificacaoSubCota,
                expenses.txtDescricaoEspecificacao,
                expenses.txtFornecedor,
                expenses.txtCNPJCPF,
                expenses.txtNumero,
                expenses.indTipoDocumento,
                expenses.datEmissao,
                expenses.vlrDocumento,
                expenses.vlrGlosa,
                expenses.vlrLiquido,
                expenses.numMes,
                expenses.numAno,
                expenses.numParcela,
                expenses.txtPassageiro,
                expenses.txtTrecho,
                expenses.numLote,
                expenses.numRessarcimento,
                expenses.vlrRestituicao,
                expenses.nuDeputadoId,
                expenses.ideDocumento,
                expenses.urlDocumento,
                deputados.uri,
                deputados.nome,
                deputados.idLegislaturaInicial,
                deputados.idLegislaturaFinal,
                deputados.nomeCivil,
                deputados.cpf,
                deputados.siglaSexo,
                deputados.urlRedeSocial,
                deputados.urlWebsite,
                deputados.dataNascimento,
                deputados.ufNascimento,
                deputados.municipioNascimento
            FROM expenses
            LEFT JOIN deputados ON expenses.txNomeParlamentar = deputados.nome
            WHERE txtDescricao = %s
            AND datEmissao BETWEEN %s AND %s
            AND txNomeParlamentar = %s
        """
        params = (description, startDate, endDate, name)
        result = execute_query(query, params, return_result=True)
        results = []
        for row in result:
            item = {
                "txNomeParlamentar": row[0],
                "cpfExpense": row[1],
                "ideCadastro": row[2],
                "nuCarteiraParlamentar": row[3],
                "nuLegislatura": row[4],
                "sgUF": row[5],
                "sgPartido": row[6],
                "codLegislatura": row[7],
                "numSubCota": row[8],
                "txtDescricao": row[9],
                "numEspecificacaoSubCota": row[10],
                "txtDescricaoEspecificacao": row[11],
                "txtFornecedor": row[12],
                "txtCNPJCPF": row[13],
                "txtNumero": row[14],
                "indTipoDocumento": row[15],
                "datEmissao": row[16].strftime('%Y-%m-%d'),
                "vlrDocumento": row[17],
                "vlrGlosa": row[18],
                "vlrLiquido": row[19],
                "numMes": row[20],
                "numAno": row[21],
                "numParcela": row[22],
                "txtPassageiro": row[23],
                "txtTrecho": row[24],
                "numLote": row[25],
                "numRessarcimento": row[26],
                "vlrRestituicao": row[27],
                "nuDeputadoId": row[28],
                "ideDocumento": row[29],
                "urlDocumento": row[30],
                "uri": row[31],
                "nome": row[32],
                "idLegislaturaInicial": row[33],
                "idLegislaturaFinal": row[34],
                "nomeCivil": row[35],
                "cpf": row[36],
                "siglaSexo": row[37],
                "urlRedeSocial": row[38],
                "urlWebsite": row[39],
                "dataNascimento": row[40].strftime('%Y-%m-%d'),
                "ufNascimento": row[41],
                "municipioNascimento": row[42],
            }
            results.append(item)

        return results
    
    return cache_function(data_function, cache_key)
    
