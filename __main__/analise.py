import os
import shutil




etl_projeto_name = 'ANTAQ'
zones =  ['bronze', 'prata', 'ouro']
ano = ['2019', '2020', '2021']

url='http://web.antaq.gov.br/Sistemas/ArquivosAnuario/Arquivos/{}.zip'
url_meta_dados = 'http://web.antaq.gov.br/Sistemas/ArquivosAnuario/Arquivos/MetadadosMovimentacao.zip'

path_parent = os.path.dirname(os.getcwd())
os.chdir(path_parent)
os.chdir(zones[0])

# print(os.getcwd())
# path_parent = os.path.dirname(os.getcwd())
# os.chdir(path_parent)
# print(os.getcwd())
# os.chdir(zones[0])
# print(os.getcwd())


file_exists = os.path.exists('readme.txt')

print(file_exists)

for a in range(len(ano)):
    try:
        #if path already exists, remove it before copying with copytree()
        if os.path.exists(dst):
            shutil.rmtree(dst)
            shutil.copytree(src, dst)
    except OSError as e:
        # If the error was caused because the source wasn't a directory
    #    else:
            print('Directory not copied. Error: %s' % e)
