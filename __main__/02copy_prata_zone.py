import shutil
import os

etl_projeto_name = 'ANTAQ'
zones =  ['bronze', 'prata', 'ouro']
ano = ['2019', '2020', '2021']

path_parent = os.path.dirname(os.getcwd())
os.chdir(path_parent)
os.chdir(zones[0])
src = os.getcwd()
os.chdir(path_parent)
os.chdir(zones[1])
dst = os.getcwd()




for a in range(len(ano)):
    src_data = (src+'\\{}.zip '.format(ano[a]))
    dst_data = (dst+'\\{}.zip '.format(ano[a]))
    shutil.copyfile(src_data,dst_data)

src_md = (src+'\\MetadadosMovimentacao.zip')
dst_md = (dst+'\\MetadadosMovimentacao.zip')
shutil.copyfile(src_md, dst_md)    
    
    