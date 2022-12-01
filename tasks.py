import tabula
import pandas as pd
from tabulate import tabulate


url = "https://www.marinha.mil.br/chm/sites/www.marinha.mil.br.chm/files/dados_de_mare/41-porto_do_rio_de_janeiro.pdf"


def extract_pdf(url: str) -> pd.DataFrame:

    # https://tabula-py.readthedocs.io/en/latest/tabula.html

    tabula.convert_into(
        url,
        "output.csv",
        output_format="csv",
        java_options="-Dfile.encoding=UTF8",
        pages="all",
        area=[
            53.6,
            27.8,
            812.8,
            94.6,
        ],  # Portion of the page to analyze(top,left,bottom,right).
        columns=[54.9, 77.8, 93.9],  # the end of column widith in pt
        guess=False,
        silent=True,
    )

    df = pd.read_csv("output.csv", sep=",", header=0)
    df.columns = ["DIA", "HORA", "ALT (m)"]

    df["HORA"] = df["HORA"].fillna("").astype(str)
    df["HORA"] = df["HORA"].apply(lambda x: "" if x == "" else (x[0:2] + ":" + x[2:4]))

    for i in range(len(df) - 1):
        dia = df.loc[i][0]
        if str(df.loc[i][0]).isnumeric():
            df.loc[i][0] = dia
        else:
            df.loc[i][0] = df.loc[i - 1][0]

    # TODO: remover linhas onde "ALT (m)" não tem valor númerico
    df[pd.to_numeric(df["ALT (m)"], errors="coerce").notnull()]

    df.dropna(inplace=True)  # remove as linhas onde não há informação de hora
    df.to_csv("output.csv", index=None)

    return df


table = tabulate(
    extract_pdf(url).head(10).fillna(""),
    headers=["DIA", "HORA", "ALT (m)"],
    tablefmt="grid",
    showindex=False,
)

print(table)
