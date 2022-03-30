import wget
import os
import shutil
import zipfile



# Bloco Definição de Variaveis
etl_projeto_name = 'ANTAQ'
zones =  ['bronze', 'prata', 'ouro']
ano = ['2019', '2020', '2021']
file_remove = ['AcordosBilaterais', 'Carga_Conteinerizada', 'Carga_Regiao', 'TemposAtracacao']
url='http://web.antaq.gov.br/Sistemas/ArquivosAnuario/Arquivos/{}.zip'
url_meta_dados = 'http://web.antaq.gov.br/Sistemas/ArquivosAnuario/Arquivos/MetadadosMovimentacao.zip'
path_parent = os.path.dirname(os.getcwd())


os.chdir(path_parent)
os.chdir(zones[0])


# Bloco Extração: Esta parte do codigo vai verificar se o item já existe, caso o item já exista ele ira remover o antigo e atualizar para um novo
if os.path.exists('MetadadosMovimentacao.zip'):
    print(f'O arquivo MetadadosMovimentacao.zip será substituido por um novo')
    os.remove('MetadadosMovimentacao.zip')
    wget.download(url_meta_dados)
else:
    print('Download: MetadadosMovimentacao.zip')
    wget.download(url_meta_dados)
    print('')

for a in range(len(ano)):
    if os.path.exists(f'{ano[a]}.zip'):
        print(f'O arquivo {ano[a]}.zip será substituido por um novo')
        os.remove(f'{ano[a]}.zip')
        wget.download(url.format(ano[a]))
        print('')
    else:
        print(f'Downloado: {ano[a]}.zip')
        wget.download(url.format(ano[a]))
        print('')


# Bloco Copy Zona Prata: Esta parte do codigo vai pegar os itens da camada bronze e copiar para zona prata, onde será feita todas as transformações necessarias
src = os.getcwd()
os.chdir(path_parent)
os.chdir(zones[1])
dst = os.getcwd()

for a in range(len(ano)):
    src_data = (src+'\\{}.zip '.format(ano[a]))
    dst_data = (dst+'\\{}.zip '.format(ano[a]))
    print('Copiando {}.zip  para camada prata'.format(ano[a]))
    shutil.copyfile(src_data,dst_data)
    print('')

src_md = (src+'\\MetadadosMovimentacao.zip')
dst_md = (dst+'\\MetadadosMovimentacao.zip')
print('Copiando MetadadosMovimentacao.zip  para camada prata')
shutil.copyfile(src_md, dst_md)    

# Bloco Unzip arquivos: Esta parte do codigo vai pegar os arquivos da zona prata e unzinpar.
print('Unzip dos arquivos da camada prata:')
with zipfile.ZipFile("MetadadosMovimentacao.zip","r") as zip_ref:
    zip_ref.extractall("MetadadosMovimentacao")

for a in range(len(ano)):
    with zipfile.ZipFile("{}.zip".format(ano[a]), 'r') as zip_ref:
        zip_ref.extractall(ano[a])



# Camada de Delete arquivos extras: Esta parte do codigo vamos deletar os arquivos que não serão usados pela equipe de DS
savePath = os.getcwd()
print('Removendo MetadadosMovimentacao.zip')
os.remove("MetadadosMovimentacao.zip")
for a in range(len(ano)):
    os.chdir(ano[a])
    for f in range(len(file_remove)):
        print("Removendo arquivos: {}{}.txt".format(ano[a], file_remove[f]))
        os.remove("{}{}.txt".format(ano[a], file_remove[f]))
    os.chdir(savePath)
    print("Removendo: {}.zip".format(ano[a]))
    os.remove("{}.zip".format(ano[a]))

