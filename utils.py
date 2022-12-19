import pandas as pd


def data_cleaning(df: pd.DataFrame) -> pd.DataFrame:
    """
    Higieniza os dados e salva em output.csv.

    Args:
        df (pd.DataFrame): DataFrame com dados da tabua de mare.
    """

    # Limpa células NaN da coluna HORA
    df["HORA"] = df["HORA"].fillna("").astype(str)

    # Formata o horário para HH:MM
    df["HORA"] = df["HORA"].apply(lambda x: "" if x == "" else (x[0:2] + ":" + x[2:4]))

    # Verifica se o valor da célula do campo DIA é numérica, caso não, preenche com o DIA anterior
    for index, row in df.iterrows():
        if str(row["DIA"]).isnumeric():
            DIA = df.at[index, "DIA"]
        else:
            df.at[index, "DIA"] = DIA

    df.dropna(inplace=True)  # remove as linhas onde não há informação de hora

    # Formata as casas decimais da coluna ALT (m)
    df["ALT (m)"] = (
        df["ALT (m)"].astype(str).apply(lambda x: ("0" + x[0:2]) if x[0] == "." else x)
    )

    df["ALT (m)"] = (
        df["ALT (m)"].astype(str).apply(lambda x: x[0:3] if len(x) > 4 else x)
    )

    df["ALT (m)"] = df["ALT (m)"].astype(float)

    return df
