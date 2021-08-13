import glob
import os
from datetime import datetime
from time import sleep

import pandas as pd
import randomheaders
import requests
import urllib3


def download():
    urllib3.disable_warnings()
    lista_de_pastas = ['arquivos_individuais', 'arquivos_combinados']
    for pasta in lista_de_pastas:
        if not os.path.isdir(pasta):
            os.mkdir(pasta)

    arquivos_individuais = 'arquivos_individuais'  # Backup dos arquivos convertidos para csv

    r_url = 'https://www.anp.gov.br/SITE/extras/consulta_petroleo_derivados/exploracao/consultaExploPocosPerfurados/planilha.asp'

    """Cria a lista dos anos para o parâmetro data = {...} em download_list
    Teoricamente, é possível fazer fazer uma requisição com todos os data
    mas extrapola os limites do servidor da ANP. Por organização, downloads
    são segmentados por ano"""

    # Todos os anos disponíveis na base da ANP
    # Para reduzir carga, modificar ano1
    ano1 = 1922
    download_list = list(range(ano1, datetime.now().year + 1))

    # Remove os anos já baixados da lista de download
    for x in [int(i.split(sep='.')[0].split('pocos')[1])
              for i in glob.glob(f'{arquivos_individuais}/*.csv')]:
        download_list.remove(x)

    # Remove os anos em que não há data
    # Incluir anos na lista para exlusão manual
    for x in [1923, 1924, 1926, 1927, 1928, 1929, 1930, 1931, 1932, 1933, 1934, 1935, 1936]:
        download_list.remove(x)

    # Adiciona os anos obrigatórios (atualiza o ano passado e o ano atual)
    download_list.extend(range(datetime.now().year - 1, datetime.now().year + 1))
    download_list = list(dict.fromkeys(download_list))

    """Baixa os arquivos em formato html e converte para csv
    Os data são exportados por meio de um script, que gera a planiha.xls.
    O código realiza a conversão de todos os arquivos para csv e mantém 
    um backup dos arquivos convertidos para consultas futuras"""

    # log
    print(f'{datetime.now()}\n'
          f' Baixando {download_list}')

    # Cria o parâmetro data_request para inserir os anos nos parâmetros da requisição
    for ano in download_list:
        data_request = {'Sim': 'Sim',
                        'txtDeOK': '01/01/{0}'.format(str(ano)),
                        'txtAteOK': '31/12/{0}'.format(str(ano)),
                        'txtBlocoOK': ''}

        """ Realiza a conexão, preservando a sessão cada vez que o código é 
        executado – s = requests.Session() – e insere os parâmetros na requisição
        – s.post() """

        s = requests.Session()
        r = s.post(r_url, headers=randomheaders.LoadHeader(), data=data_request, verify=False, stream=True)

        # Baixa os arquivos no formato original (html)
        try:
            arquivo_html = f'{arquivos_individuais}/pocos{str(ano)}.html'
            with open(arquivo_html, 'wb') as output:
                output.write(r.content)
            # Converte o arquivo html para csv
            df = pd.read_html(arquivo_html, decimal=',', encoding='latin1', thousands='.')[0]
            arquivo_csv0 = os.path.basename(arquivo_html).split('.')[0]
            arquivo_csv = f'{arquivo_csv0}.csv'
            df.to_csv(f'{arquivos_individuais}/{arquivo_csv}', index=False, header=False, encoding='latin1',
                      decimal=',')
            sleep(1)
        finally:
            print(f'{ano}: {round(os.path.getsize(arquivo_html) / 1024)} kb; status_code: {r.status_code}')

        # Remove os arquivos html e arquivos sem data (<430b)
        os.remove(arquivo_html)
        for x in glob.glob(f'{arquivos_individuais}/*.*'):
            if os.path.getsize(x) <= 430:
                os.remove(x)
                print(f'{x} removido por falha de conexão (resposta em branco)')


def merge():
    csv_list = glob.glob('arquivos_individuais/*.csv')
    df_list = []
    print(f'Combinando arquivos')
    for csv_file in csv_list:
        df = pd.read_csv(csv_file, encoding='cp1252', parse_dates=False)
        df_list.append(df)
        df = pd.concat(df_list, axis=0)
        df.to_csv(f"arquivos_combinados/pocos_bruto_{datetime.today().strftime('%m_%d_%Y')}.csv", encoding='cp1252')
