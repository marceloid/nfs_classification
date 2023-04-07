import re

import pandas as pd

from pathlib import Path
from time import time

from nfs_classification.nomes_comuns import get_lista_nomes_comuns


def replace_names(row, regex):
    print(row.name)
    return re.sub(regex, '#NOME#', row['discriminacao_anonimizado'], flags=re.IGNORECASE)


def generate_anonimized_df():
    time_ini = time()
    pickle_file = Path('poc3v2.pickle')

    colunas_tipos = {
        'idn_empr': object,
        'cod_ano_nume': object,
        'val_liqd': float,
        'val_aliq': float,
        'val_iss': float,
        'tipo_recolhimento': object,
        'cod_ctiss': object,
        'lista_servico': object,
        'natureza_operacao': object,
        'regime_esp_trib': object,
        'discriminacao': object,
    }

    colunas_datas = ['data_emissao', 'data_competencia']

    if not pickle_file.exists():
        df_nfse = pd.read_csv(
            'poc3v2.csv',
            on_bad_lines='warn',
            sep='|',
            dtype=colunas_tipos,
            parse_dates=colunas_datas,
        )
        df_nfse.to_pickle('poc3v2.pickle')
    else:
        df_nfse = pd.read_pickle(pickle_file)

    df_unicos = df_nfse.copy()
    print(f'Tempo para abertura do arquivo: {time() - time_ini}')
    time_ini = time()
    df_unicos['discriminacao'] = df_unicos['discriminacao'].fillna('')

    df_unicos['discriminacao_anonimizado'] = df_unicos['discriminacao'].apply(
        lambda x: re.sub(r'\d', '9', x)
    )
    print(f'Tempo para anonimização dos dígitos: {time() - time_ini}')

    time_ini = time()
    df_unicos['discriminacao_anonimizado'] = df_unicos['discriminacao_anonimizado'].apply(
        lambda x: re.sub(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', 'aaa@bbb.ccc', x)
    )
    print(f'Tempo para anonimização dos emails: {time() - time_ini}')

    time_ini = time()
    df_unicos = (
        df_unicos.drop_duplicates(subset=['lista_servico', 'discriminacao_anonimizado'])
        .copy()
        .reset_index(drop=True)
    )
    print(f'Tempo para remoção de duplicidades: {time() - time_ini}')

    time_ini = time()
    nomes_comuns_br = get_lista_nomes_comuns('nomes_comuns_br_IBGE_2023-04-06.pickle')
    nomes_comuns_br_freq_maior_que_100 = [
        nome
        for nome in nomes_comuns_br[nomes_comuns_br['freq'] > 100]['nome'].to_list()
        if len(nome) > 3
    ]
    print(f'Tempo para obtenção dos nomes brasileiros: {time() - time_ini}')

    time_ini = time()
    regex_nomes = r'\b(' + r'|'.join(nomes_comuns_br_freq_maior_que_100) + r')\b'

    df_unicos['discriminacao_anonimizado_sem_nomes'] = df_unicos.apply(
        lambda x: replace_names(x, regex_nomes), axis='columns'
    )
    print(f'Tempo para anonimização dos nomes brasileiros: {time() - time_ini}')

    df_unicos = (
        df_unicos.drop_duplicates(subset=['lista_servico', 'discriminacao_anonimizado'])
        .copy()
        .reset_index(drop=True)
    )

    time_ini = time()
    df_unicos.to_pickle('df_anonimizado.pickle')
    print(f'Tempo para gravação do arquivo binário pickle: {time() - time_ini}')

    time_ini = time()
    df_unicos.to_excel('df_anonimizado.xlsx')
    print(f'Tempo para gravação do arquivo Excel: {time() - time_ini}')


if __name__ == '__main__':
    anonimized_df = Path('df_anonimizado.pickle')
    if anonimized_df.exists():
        print('Arquivo já existe. Lendo arquivo binário pickle...')

        time_ini = time()
        df_unicos = pd.read_pickle(anonimized_df)
        print(f'Tempo para leitura do arquivo binário pickle: {time() - time_ini}')

        time_ini = time()
        df_unicos.to_excel('df_anonimizado.xlsx')
        print(f'Tempo para gravação do arquivo Excel: {time() - time_ini}')
