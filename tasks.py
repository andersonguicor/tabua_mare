# -*- coding: utf-8 -*-
"""
Tábuas de Maré task
"""
import ssl

import tabula
import pandas as pd
import numpy as np
from prefect import task

from utils import data_cleaning, log


@task
def parse_data(url: str) -> pd.DataFrame:
    """
    Converte os dados do pdf em um DataFrame do Pandas.

    Args:
        url (str): Número da página a ser analisada.

    Returns:
        pd.DataFrame: DataFrame do Pandas.
    """

    # https://tabula-py.readthedocs.io/en/latest/tabula.html
    # Pagina 1
    ssl._create_default_https_context = ssl._create_unverified_context
    dt1 = tabula.read_pdf(
        url,
        output_format="dataframe",
        pandas_options={"dtype": str},
        pages=1,
        # Delimitando a área da página a ser analisada
        # (top,left,bottom,right) em pt.
        area=[
            53.6,
            27.8,
            803.8,
            566.3,
        ],
        # Posição final de cada coluna em pt
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
        ],
        guess=False,
        silent=True,
    )

    # Pagina 2
    dt2 = tabula.read_pdf(
        url,
        output_format="dataframe",
        pandas_options={"dtype": str},
        pages=2,
        # Delimitando a área da página a ser analisada
        # (top,left,bottom,right) em pt.
        area=[
            53.6,
            27.8,
            803.8,
            566.3,
        ],
        # Posição final de cada coluna em pt
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
        ],
        guess=False,
        silent=True,
    )

    # Pagina 3
    dt3 = tabula.read_pdf(
        url,
        output_format="dataframe",
        pandas_options={"dtype": str},
        pages=3,
        # Delimitando a área da página a ser analisada
        # (top,left,bottom,right) em pt.
        area=[
            53.6,
            27.8,
            803.8,
            566.3,
        ],
        # Posição final de cada coluna em pt
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
        ],
        guess=False,
        silent=True,
    )
    df1 = pd.DataFrame(dt1[0])
    df2 = pd.DataFrame(dt2[0])
    df3 = pd.DataFrame(dt3[0])
    df_list = [df1, df2, df3]
    dataframe = pd.concat(
        df_list,
        ignore_index=True,
    )
    arange = np.arange(len(dataframe.columns))
    dataframe.index = pd.CategoricalIndex(
        dataframe.index, ordered=True, categories=dataframe.index.unique()
    )
    dataframe.columns = [arange // 3, arange % 3]
    dataframe = (
        dataframe.stack(0).sort_index(level=1).reset_index(level=1, drop=True)
    )
    dataframe.columns = ["DIA", "HORA", "ALT (m)"]
    dataframe = dataframe.reset_index()
    del dataframe["index"]

    log("Dados convertidos em DataFrame com sucesso!")

    return data_cleaning(dataframe)


@task
def save_report(dfm: pd.DataFrame) -> None:
    """
    Salva o DataFrame em um arquivo CSV.

    Args:
        dfm (pd.DataFrame): DataFrame do Pandas.
    """
    dfm["DIA"] = dfm["DIA"].astype(str)
    dfm.to_csv("../output.csv", index=False)
    log("Dados salvos em output.csv com sucesso!")
