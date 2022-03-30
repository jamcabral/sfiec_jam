import shutil
import os

etl_projeto_name = 'ANTAQ'
zones =  ['bronze', 'prata', 'ouro']
ano = ['2019', '2020', '2021']
file_cp = ['Atracacao', 'Carga']

path_parent = os.path.dirname(os.getcwd())
os.chdir(path_parent)
os.chdir(zones[2])
dst = os.getcwd()
os.chdir(path_parent)
os.chdir(zones[1])
src = os.getcwd()


print(src)
print(dst)


try:
    #if path already exists, remove it before copying with copytree()
    if os.path.exists(dst):
        shutil.rmtree(dst)
        shutil.copytree(src, dst)
except OSError as e:
    # If the error was caused because the source wasn't a directory
#    else:
        print('Directory not copied. Error: %s' % e)


#shutil.copytree(src, dst, ignore = shutil.ignore_patterns('a'),  follow_symlinks=True)

# for a in range(len(ano)):
#     print(os.getcwd())
#     print(ano[a])
#     for f in range(len(file_cp)):
#         print(file_cp[f])
#         if os.path.isdir(ano[a]):
#             print('diretorio existe')
#         else:
#             print('diretorio nao existe')
#         # src_data = (src+'\\{}\\{}{}.txt'.format(ano[a], ano[a], file_cp[f]))
#         # dst_data = (dst+'\\{}\\{}{}.txt'.format(ano[a], ano[a], file_cp[f]))
#         # shutil.copyfile(src_data,dst_data)   
#     os.chdir(src)

    