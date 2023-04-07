import requests

import pandas as pd

from pathlib import Path


def get_lista_nomes_comuns(arquivo=None):
    if not arquivo:
        url_feminino = (
            'http://servicodados.ibge.gov.br/api/v1/censos/nomes/ranking?qtd=200000&sexo=f'
        )
        url_masculino = (
            'http://servicodados.ibge.gov.br/api/v1/censos/nomes/ranking?qtd=200000&sexo=m'
        )

        response_feminino = requests.get(url_feminino)
        response_masculino = requests.get(url_masculino)

        dados_femininos = pd.DataFrame(response_feminino.json())

        dados_masculinos = pd.DataFrame(response_masculino.json())

        return pd.concat([dados_femininos, dados_masculinos])

    arquivo_nomes = Path(arquivo)
    if arquivo_nomes.exists():
        return pd.read_pickle(arquivo_nomes)
