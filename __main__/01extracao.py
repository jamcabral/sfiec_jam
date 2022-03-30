import wget
import os


etl_projeto_name = 'ANTAQ'
zones =  ['bronze', 'prata', 'ouro']
ano = ['2019', '2020', '2021']

url='http://web.antaq.gov.br/Sistemas/ArquivosAnuario/Arquivos/{}.zip'
url_meta_dados = 'http://web.antaq.gov.br/Sistemas/ArquivosAnuario/Arquivos/MetadadosMovimentacao.zip'


path_parent = os.path.dirname(os.getcwd())
os.chdir(path_parent)
os.chdir(zones[0])





print('Download Meta Dados --')
wget.download(url_meta_dados)
print('')
for a in range(len(ano)):
    print(ano[a])
    wget.download(url.format(ano[a]))
    print('')

#with zipfile.ZipFile("file.zip", 'r') as zip_ref:
 #   zip_ref.extractall("targetdir")