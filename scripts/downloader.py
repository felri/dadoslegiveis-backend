import urllib.request
import datetime
import pandas as pd
import os
import schedule
import time
import zipfile
from db import deputados, expenses
from unidecode import unidecode


URL_DEPUTIES = "https://dadosabertos.camara.leg.br/arquivos/deputados/csv/deputados.csv"
URL_EXPENSES = "https://www.camara.leg.br/cotas/"

YEARS = [
    # 2008,
    # 2009,
    # 2010,
    # 2011,
    # 2012,
    # 2013,
    # 2014,
    # 2015,
    # 2016,
    # 2017,
    # 2018,
    # 2019,
    # 2020,
    # 2021,
    # 2022,
    2023,
]

INTEGER_FIELDS = [
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

FLOAT_FIELDS = [
    "vlrRestituicao",
    "vlrGlosa",
    "vlrLiquido",
]

DATE_FIELDS = [
    "datEmissao",
]

NOT_FOUND_DEPUTIES = [
    "LIDMIN",
    "Rodrigo Maia",
]


PARTY_LEADERS = {
    "LIDERANÇA DO CIDADANIA": "Alex Manente",
    "LIDERANÇA DO DEMOCRATAS": "Miguel Haddad",
    "LIDERANÇA DO NOVO": "Tiago Mitraud",
    "LIDERANÇA DO PARTIDO REPUBLICANO DA ORDEM SOCIAL": "Weliton Prado",
    "LIDERANÇA DO PDT": "Andre Figueiredo",
    "LIDERANÇA DO PODEMOS": "Igor Timo",
    "LIDERANÇA DO PROGRESSISTAS": "Andre Fufuca",
    "LIDERANÇA DO PSC": "Euclydes Pettersen",
    "LIDERANÇA DO PSDB": "Adolfo Viana",
    "LIDERANÇA DO PSD": "Antonio Brito",
    "LIDERANÇA DO PSL": "Delegado Waldir",
    "LIDERANÇA DO PSOL": "Samia Bomfim",
    "LIDERANÇA DO PT": "Reginaldo Lopes",
    "LIDERANÇA DO PTB": "Paulo Bengtson",
    "LIDERANÇA DO REPUBLICANOS": "Vinicius Carvalho",
    "LIDERANÇA DO SOLIDARIEDADE": "Lucas Vergilio",
    "LIDERANÇA DO UNIÃO BRASIL": "Elmar Nascimento",
}

PARTIES_FOR_LEADERS = {
    "LIDERANÇA DO CIDADANIA": "CIDADANIA",
    "LIDERANÇA DO DEMOCRATAS": "DEMOCRATAS",
    "LIDERANÇA DO NOVO": "NOVO",
    "LIDERANÇA DO PARTIDO REPUBLICANO DA ORDEM SOCIAL": "PROS",
    "LIDERANÇA DO PDT": "PDT",
    "LIDERANÇA DO PODEMOS": "PODEMOS",
    "LIDERANÇA DO PROGRESSISTAS": "PROGRESSISTAS",
    "LIDERANÇA DO PSC": "PSC",
    "LIDERANÇA DO PSDB": "PSDB",
    "LIDERANÇA DO PSD": "PSD",
    "LIDERANÇA DO PSL": "PSL",
    "LIDERANÇA DO PSOL": "PSOL",
    "LIDERANÇA DO PT": "PT",
    "LIDERANÇA DO PTB": "PTB",
    "LIDERANÇA DO REPUBLICANOS": "REPUBLICANOS",
    "LIDERANÇA DO SOLIDARIEDADE": "SOLIDARIEDADE",
    "LIDERANÇA DO UNIÃO BRASIL": "UNIÃO",
}

DIRECTORY = "./datasets/expenses"


def download_deputies():
    # Extract the file name from the URL
    file_name = URL_DEPUTIES.split("/")[-1]
    # Create the datasets directory if it doesn't exist
    # Download the file and save it to the datasets directory
    urllib.request.urlretrieve(URL_DEPUTIES, f"./datasets/{file_name}")
    print(f"File {file_name} downloaded to ./datasets/{file_name}")
    deputados.load_data(f"datasets/{file_name}")
    return


def download_expenses(years=YEARS):
    # Create the datasets directory if it doesn't exist
    # expenses.create_table()
    os.makedirs("expenses", exist_ok=True)
    for year in years:
        file_url = f"{URL_EXPENSES}Ano-{year}.csv.zip"
        print(f"Downloading file {file_url}...")
        zip_file = f"{DIRECTORY}/Ano-{year}.csv.zip"
        file_name = f"Ano-{year}.csv"

        # Check if the file already exists in the folder
        if os.path.exists(f"{DIRECTORY}/{file_name}"):
            os.remove(f"{DIRECTORY}/{file_name}")
            print(f"File {file_name} deleted")

        urllib.request.urlretrieve(file_url, zip_file)
        print(f"File {zip_file} downloaded to /expenses/{zip_file}")

        # Unzip the downloaded file
        with zipfile.ZipFile(zip_file, "r") as zip_ref:
            zip_ref.extractall(DIRECTORY)
        print(f"File {zip_file} unzipped to datasets/expenses/{file_name}")

        # Delete the zip file
        os.remove(zip_file)
        print(f"File {zip_file} deleted")
    return


def clean_csv(csv):
    df = pd.read_csv(csv, sep=";", encoding="utf-8")
    print(df)
    df[INTEGER_FIELDS] = df[INTEGER_FIELDS].fillna(0)
    df[FLOAT_FIELDS] = df[FLOAT_FIELDS].fillna(0.0)
    # fill empty sgPartido with the party of the leader,
    # but only for the leaders, the rest should stay the same
    df["sgPartido"] = df.apply(
        lambda row: PARTIES_FOR_LEADERS[row["txNomeParlamentar"]]
        if row["txNomeParlamentar"] in PARTY_LEADERS
        else row["sgPartido"],
        axis=1,
    )
    df["txNomeParlamentar"] = df["txNomeParlamentar"].apply(unidecode)
    df = df.iloc[1:]
    df[INTEGER_FIELDS] = df[INTEGER_FIELDS].astype(int)
    df[FLOAT_FIELDS] = df[FLOAT_FIELDS].astype(float)
    df = remove_duplicates(df)
    return df


def remove_duplicates(df):
    index = [
        "txtNumero",
        "vlrDocumento",
        "datEmissao",
        "txtCNPJCPF",
        "txNomeParlamentar",
        "numMes",
        "numAno",
        "numParcela",
        "numLote",
        "numRessarcimento",
        "vlrRestituicao",
        "nuDeputadoId",
        "ideDocumento",
        "urlDocumento",
    ]

    df = df.drop_duplicates(subset=index, keep="first")
    return df


def save_csv(df, csv, directory=DIRECTORY):
    # Save the cleaned csv file
    if os.path.exists(f"{DIRECTORY}/updated-{csv}"):
        os.remove(f"{directory}/updated-{csv}")
    if os.path.exists(f"{DIRECTORY}/{csv}"):
        os.remove(f"{directory}/{csv}")
    df.to_csv(f"{directory}/updated-{csv}", index=False, header=True)
    print(f"File {csv} saved to {directory}/{csv}")
    return


def format_csv_data_to_db():
    # Format the csv data to be inserted into the database
    for csv in os.listdir(DIRECTORY):
        if csv.endswith(".csv"):
            df = clean_csv(f"{DIRECTORY}/{csv}")
            save_csv(df, csv)
    return


def download_expenses_current_year():
    # Get current year
    remove_all_files()
    print("Downloading expenses for the current year")
    current_year = datetime.datetime.now().year
    # Download expenses for the current year
    download_expenses([current_year])


def remove_all_files():
    # Remove all files from the datasets folder
    for file in os.listdir(DIRECTORY):
        # remove all csv files from the datasets folder
        if file.endswith(".csv"):
            os.remove(os.path.join(DIRECTORY, file))
    return