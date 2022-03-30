import os

etl_projeto_name = 'ANTAQ'
zones =  ['bronze', 'prata', 'ouro']
ano = ['2019', '2020', '2021']
file_remove = ['AcordosBilaterais', 'Carga_Conteinerizada', 'Carga_Regiao', 'TemposAtracacao']

path_parent = os.path.dirname(os.getcwd())
os.chdir(path_parent)
os.chdir(zones[1])
savePath = os.getcwd()


os.remove("MetadadosMovimentacao.zip")

for a in range(len(ano)):
    print(ano[a])
    os.chdir(ano[a])
    print(os.getcwd())
    for f in range(len(file_remove)):
        os.remove("{}{}.txt".format(ano[a], file_remove[f]))
    os.chdir(savePath)
    os.remove("{}.zip".format(ano[a]))




