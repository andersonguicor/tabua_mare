import tabula
import pandas as pd
from tabulate import tabulate

from utils import data_cleaning
from urllib import request
import numpy as np

# Arquivo remoto a ser baixado
url = "https://www.marinha.mil.br/chm/sites/www.marinha.mil.br.chm/files/dados_de_mare/41-porto_do_rio_de_janeiro.pdf"


def download_pdf(remote_url: str):
    """
    Baixa o pdf com a tabua das mares.

    Args:
        remote_url (str): url do arquivo pdf.
    """
    import ssl

    ssl._create_default_https_context = ssl._create_unverified_context

    # Nome do arquivo local para salvar os dados
    local_file = "tabua_mare.pdf"
    # Baixa remotamente e salva localmente
    request.urlretrieve(remote_url, local_file)


def parse_data(remote_url: str) -> pd.DataFrame:
    """
    Transforma os dados em formato CSV em um DataFrame do Pandas.

    Args:
        file (str): caminho d.

    Returns:
        pd.DataFrame: DataFrame do Pandas.
    """

    # https://tabula-py.readthedocs.io/en/latest/tabula.html
    dt = tabula.read_pdf(
        "./tabua_mare.pdf",
        output_format="dataframe",
        pandas_options={"dtype": str},
        pages="1",
        area=[
            53.6,
            27.8,
            803.8,
            566.3,
        ],  # Parte da página a ser analisada (top,left,bottom,right) em pt.
        columns=[
            54.9,
            77.8,
            93.9,
            119.8,
            145.8,
            159.4,
            185.8,
            210.2,
            227.0,
            251.8,
            278.2,
            293.4,
            320.0,
            345.0,
            360.6,
            385.4,
            411.4,
            427.0,
            454.2,
            477.8,
            493.4,
            521.8,
            544.6,
            566.2,
        ],  # posição final de cada coluna em pt
        guess=False,
        silent=True,
    )
    dataframe = pd.DataFrame(dt[0])

    a = np.arange(len(dataframe.columns))
    dataframe.index = pd.CategoricalIndex(
        dataframe.index, ordered=True, categories=dataframe.index.unique()
    )
    dataframe.columns = [a // 3, a % 3]
    dataframe = dataframe.stack(0).sort_index(level=1).reset_index(level=1, drop=True)
    dataframe.columns = ["DIA", "HORA", "ALT (m)"]
    dataframe = dataframe.reset_index()
    del dataframe["index"]

    return data_cleaning(dataframe)


# visualizando os dados. TODO apagar
table = tabulate(
    parse_data(url).fillna(""),
    headers=["DIA", "HORA", "ALT (m)"],
    tablefmt="grid",
    showindex=False,
)

print(table)
