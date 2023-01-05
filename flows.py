# -*- coding: utf-8 -*-
"""
Tábuas de Maré flow
"""

from prefect import Flow

from tasks import (
    parse_data,
    save_report,
)

with Flow("anderson") as flow:
    # Tasks
    URL = "https://www.marinha.mil.br/chm/sites/www.marinha.mil.br.chm/files/dados_de_mare/41-porto_do_rio_tabua_2023_0.pdf"  # noqa: E501 # pylint: disable=line-too-long
    dataframe = parse_data(URL)
    save_report(dataframe)
