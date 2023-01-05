# -*- coding: utf-8 -*-
"""
Tábuas de Maré utils
"""

import pandas as pd
import prefect

def log(message) -> None:
    """Logs a message"""
    prefect.context.logger.info(f"\n{message}")

def data_cleaning(dfm: pd.DataFrame) -> pd.DataFrame:
    """
    Higieniza os dados e salva em output.csv.

    Args:
        df (pd.DataFrame): DataFrame com dados da tabua de mare.
    """

    # Limpa células NaN da coluna HORA
    dfm["HORA"] = dfm["HORA"].fillna("").astype(str)

    # Formata o horário para HH:MM
    dfm["HORA"] = dfm["HORA"].apply(
        lambda x: "" if x == "" else (x[0:2] + ":" + x[2:4])
    )

    # Considera a informação da linha anterior para a linha atual
    # quando o campo hora estiver vazio
    for index, row in dfm.iterrows():
        if str(row["DIA"]).isnumeric() and row["HORA"]=="":
            dia = dfm.at[index-1, "DIA"] = dfm.at[index, "DIA"]


    # Verifica se o valor da célula do campo DIA é numérico
    # caso não, preenche com o DIA anterior
    for index, row in dfm.iterrows():
        if str(row["DIA"]).isnumeric():
            dia = dfm.at[index, "DIA"]
        else:
            dfm.at[index, "DIA"] = dia

    dfm.dropna(inplace=True)  # remove as linhas onde não há informação de hora

    # Formata as casas decimais da coluna ALT (m)
    dfm["ALT (m)"] = (
        dfm["ALT (m)"]
        .astype(str)
        .apply(lambda x: ("0" + x[0:2]) if x[0] == "." else x)
    )

    dfm["ALT (m)"] = (
        dfm["ALT (m)"].astype(str).apply(lambda x: x[0:3] if len(x) > 4 else x)
    )

    dfm["DIA"] = dfm["DIA"].astype(str)
    dfm["ALT (m)"] = dfm["ALT (m)"].astype(float)

    
    return dfm
