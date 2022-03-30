import zipfile
import os

etl_projeto_name = 'ANTAQ'
zones =  ['bronze', 'prata', 'ouro']
ano = ['2019', '2020', '2021']

path_parent = os.path.dirname(os.getcwd())
os.chdir(path_parent)
os.chdir(zones[1])

with zipfile.ZipFile("MetadadosMovimentacao.zip","r") as zip_ref:
    zip_ref.extractall("MetadadosMovimentacao")

for a in range(len(ano)):
    with zipfile.ZipFile("{}.zip".format(ano[a]), 'r') as zip_ref:
        zip_ref.extractall(ano[a])


