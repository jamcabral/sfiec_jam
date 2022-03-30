import pandas as pd
import os
import pyodbc
import numpy as np
import re
import shutil

savepath = os.getcwd()
os.chdir("..")
os.chdir('prata')
zone_prata = os.getcwd()
os.chdir('MetadadosMovimentacao')
zone_meta_dados = os.getcwd()
os.chdir("..")
os.chdir("..")
os.chdir('ouro')
zone_ouro = os.getcwd()
print(savepath)
print(zone_prata)
print(zone_ouro)
print(zone_meta_dados)

etl_projeto_name = 'ANTAQ'
zones =  ['bronze', 'prata', 'ouro']
ano = ['2019', '2020', '2021']
file_remove = ['AcordosBilaterais', 'Carga_Conteinerizada', 'Carga_Regiao', 'TemposAtracacao']
file_exist = ['Atracacao', 'Carga']
columns_atracacao = ['IDAtracacao', 'CDTUP', 'IDBerco', 'Berço', 'Porto Atracação','Apelido Instalação Portuária', 'Complexo Portuário', 'Tipo da Autoridade Portuária', 'Data Atracação'
                  ,'Data Chegada', 'Data Desatracação', 'Data Início Operação', 'Data Término Operação', 'Tipo de Operação', 'SGUF', 'Região Geográfica']
columns_carga = ['IDCarga', 'IDAtracacao', 'Origem', 'Destino', 'CDMercadoria', 'Tipo Operação da Carga', 'Carga Geral Acondicionamento', 'ConteinerEstado', 'Tipo Navegação'
                , 'FlagAutorizacao', 'FlagCabotagem', 'FlagCabotagemMovimentacao', 'FlagConteinerTamanho', 'FlagLongoCurso', 'FlagMCOperacaoCarga', 'FlagOffshore']

os.chdir(zone_prata)
os.chdir('2019')
atracacao_fato_2019 = pd.read_csv('2019Atracacao.txt', sep=";", error_bad_lines=False, index_col=False, dtype='unicode')
carga_fato_2019 = pd.read_csv('2019Carga.txt', sep=";", error_bad_lines=False, index_col=False, dtype='unicode')
os.chdir(zone_prata)
os.chdir('2020')
atracacao_fato_2020 = pd.read_csv('2020Atracacao.txt', sep=";", error_bad_lines=False, index_col=False, dtype='unicode')
carga_fato_2020 = pd.read_csv('2020Carga.txt', sep=";", error_bad_lines=False, index_col=False, dtype='unicode')
os.chdir(zone_prata)
os.chdir('2021')
atracacao_fato_2021 = pd.read_csv('2021Atracacao.txt', sep=";", error_bad_lines=False, index_col=False, dtype='unicode')
carga_fato_2021 = pd.read_csv('2021Carga.txt', sep=";", error_bad_lines=False, index_col=False, dtype='unicode')

atracacao_fato = pd.concat([atracacao_fato_2019, atracacao_fato_2020, atracacao_fato_2021], ignore_index=True, sort=False)
carga_fato = pd.concat([carga_fato_2019, carga_fato_2020, carga_fato_2021], ignore_index=True, sort=False)

os.chdir(zone_meta_dados)
meta_dados_atracao = pd.read_csv('MetadadosAtracacao.txt', sep=";")
meta_dados_carga = pd.read_csv('MetadadosCarga.txt', sep=";")

pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_columns', None)
pd.set_option("display.max_rows", 100)

# meta_dados_atracao.head(24)
# meta_dados_carga.head(30)

atracacao_fato = atracacao_fato[columns_atracacao]
carga_fato = carga_fato[columns_carga]

atracacao_fato['Data Atracação'] = pd.to_datetime(atracacao_fato['Data Atracação'], errors='coerce', format='%d/%m/%Y %H:%M:%S')
atracacao_fato['Data Chegada'] = pd.to_datetime(atracacao_fato['Data Chegada'],  errors='coerce', format='%d/%m/%Y %H:%M:%S')
atracacao_fato['Data Desatracação'] = pd.to_datetime(atracacao_fato['Data Desatracação'],  errors='coerce', format='%d/%m/%Y %H:%M:%S')
atracacao_fato['Data Início Operação'] = pd.to_datetime(atracacao_fato['Data Início Operação'],  errors='coerce', format='%d/%m/%Y %H:%M:%S')
atracacao_fato['Data Término Operação'] = pd.to_datetime(atracacao_fato['Data Término Operação'],  errors='coerce', format='%d/%m/%Y %H:%M:%S')
atracacao_fato['Ano da data de início da operação'] = atracacao_fato['Data Início Operação'].dt.year
atracacao_fato['Mês da data de início da operação'] = atracacao_fato['Data Início Operação'].dt.month

atracacao_fato = atracacao_fato.where(pd.notnull(atracacao_fato), None)
atracacao_fato['Data Atracação'].replace({np.nan:None}, inplace=True)
atracacao_fato['Data Chegada'].replace({np.nan:None}, inplace=True)
atracacao_fato['Data Desatracação'].replace({np.nan:None}, inplace=True)
atracacao_fato['Data Início Operação'].replace({np.nan:None}, inplace=True)
atracacao_fato['Data Término Operação'].replace({np.nan:None}, inplace=True)

carga_fato.replace({np.nan:None}, inplace=True)

server = 'localhost' 
database = 'sfiec' 
username = 'sa' 
password = 'Sfiec@*ANTAQ'  
cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

# cursor.execute('DROP TABLE IF EXISTS ATRACACAO_FATO')
# cnxn.commit()

# cursor.execute('''
# CREATE TABLE ATRACACAO_FATO(
# [ATCF_IDA] INT
# ,[ATC_CDTUP] VARCHAR(100)
# ,[ATC_IDB] VARCHAR(100)
# ,[ATC_BERCO] VARCHAR(100)
# ,[ATC_PRT_ATR] VARCHAR(100)
# ,[ATC_AIP] VARCHAR(100)
# ,[ATC_CP] VARCHAR(100)
# ,[ATC_TAP] VARCHAR(100)
# ,[ATC_DT_ATR] DATETIME
# ,[ATC_DT_CHE] DATETIME
# ,[ATC_DT_DES] DATETIME
# ,[ATC_DT_INIOP] DATETIME
# ,[ATC_DT_TERMOF] DATETIME
# ,[ATC_T_OP] VARCHAR(50)
# ,[ATC_SGUF] VARCHAR(3)
# ,[ATC_RGEO] VARCHAR(20)
# ,[ATC_A_DT_INI_OP]  VARCHAR(10)
# ,[ATC_M_DT_INI_OP] VARCHAR(5)
# )''')
# cnxn.commit()

# cursor.execute('DROP TABLE IF EXISTS CARGA_FATO')
# cnxn.commit()

# cursor.execute('''
# CREATE TABLE CARGA_FATO(
# [CG_IDC] INT
# ,[CG_IDA] INT
# ,[CG_ORG] VARCHAR(50)
# ,[CG_DST] VARCHAR(50)
# ,[CG_CDMERC] VARCHAR(10)
# ,[CG_TIP_OP] VARCHAR(100)
# ,[CG_ACMT] VARCHAR(50)
# ,[CG_CNT_EST] VARCHAR(50)
# ,[CG_TP_NAV] VARCHAR(50)
# ,[CG_F_AUT] VARCHAR(10)
# ,[CG_F_CAB] VARCHAR(10)
# ,[CG_F_CAB_MOV] VARCHAR(10)
# ,[CG_F_CNT_TAM] VARCHAR(10)
# ,[CG_F_LON_CURS] VARCHAR(10)
# ,[CG_F_MC_OP] VARCHAR(10)
# ,[CG_F_OFF_SHO] VARCHAR(10)
# )''')
# cnxn.commit()

# FULL Insert ATRACACAO FATO in SQL SERVER 
# for index, row in atracacao_fato.iterrows():
#      cursor.execute("INSERT INTO ATRACACAO_FATO (ATCF_IDA, ATC_CDTUP, ATC_IDB, ATC_BERCO, ATC_PRT_ATR, ATC_AIP, ATC_CP, ATC_TAP, ATC_DT_ATR, ATC_DT_CHE, ATC_DT_DES, ATC_DT_INIOP, ATC_DT_TERMOF, ATC_T_OP, ATC_SGUF ,ATC_RGEO, ATC_A_DT_INI_OP, ATC_M_DT_INI_OP ) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", 
#                         row['IDAtracacao'], 
#                         row['CDTUP'], 
#                         row['IDBerco'], 
#                         row['Berço'], 
#                         row['Porto Atracação'], 
#                         row['Apelido Instalação Portuária'], 
#                         row['Complexo Portuário'], 
#                         row['Tipo da Autoridade Portuária'], 
#                         row['Data Atracação'], 
#                         row['Data Chegada'], 
#                         row['Data Desatracação'], 
#                         row['Data Início Operação'], 
#                         row['Data Término Operação'], 
#                         row['Tipo de Operação'], 
#                         row['SGUF'], 
#                         row['Região Geográfica'], 
#                         row['Ano da data de início da operação'], 
#                         row['Mês da data de início da operação'])
# cnxn.commit()
# cursor.close()

# FULL Insert CARGA FATO in SQL SERVER 
# %%time
# for index, row in carga_fato.iterrows():
#      cursor.execute("INSERT INTO ATRACACAO_FATO (CG_IDC, CG_IDA, CG_ORG, CG_DST, CG_CDMERC, CG_TIP_OP, CG_ACMT, CG_CNT_EST, CG_TP_NAV, CG_F_AUT, CG_F_CAB, CG_F_CAB_MOV, CG_F_CNT_TAM, CG_F_LON_CURS, CG_F_MC_OP, CG_F_OFF_SHO ) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", 
#                         row['IDCarga'], 
#                         row['IDAtracacao'], 
#                         row['Origem'], 
#                         row['Destino'], 
#                         row['CDMercadoria'],  
#                         row['Tipo Operação da Carga'],  
#                         row['Carga Geral Acondicionamento'],  
#                         row['ConteinerEstado'],  
#                         row['Tipo Navegação'],  
#                         row['FlagAutorizacao'],  
#                         row['FlagCabotagem'],  
#                         row['FlagCabotagemMovimentacao'],  
#                         row['FlagConteinerTamanho'],  
#                         row['FlagLongoCurso'],  
#                         row['FlagMCOperacaoCarga'],  
#                         row['FlagOffshore'])
# cnxn.commit()
# cursor.close()


#Query dados do Ceará com a coluna UF no banco de dados
query_ceara= """select ATC_CP AS 'Localidade',
count(*) AS 'Número de Atracações', 
ATC_CDTUP, 
AVG(DATEDIFF( HOUR , ATC_DT_CHE , ATC_DT_INIOP )) AS 'Tempo de espera médio em horas', 
AVG(DATEDIFF( HOUR , ATC_DT_ATR , ATC_DT_DES )) AS 'Tempo atracado médio em horas', 
ATC_M_DT_INI_OP  AS 'Mes', 
ATC_A_DT_INI_OP AS 'ANO' 
from ATRACACAO_FATO
WHERE ATC_A_DT_INI_OP IN ('2020', '2021') 
AND ATC_SGUF = 'CE' AND ATC_DT_CHE IS NOT NULL AND ATC_DT_INIOP IS NOT NULL
GROUP BY ATC_CDTUP, ATC_CP, ATC_M_DT_INI_OP, ATC_A_DT_INI_OP
ORDER BY 4 ASC"""

#Query dados do Nordeste sem a coluna Regiao no banco de dados
query_Nordeste = """select ATC_CP AS 'Localidade',
count(*) AS 'Número de Atracações', 
ATC_CDTUP, 
AVG(DATEDIFF( HOUR , ATC_DT_CHE , ATC_DT_INIOP )) AS 'Tempo de espera médio em horas', 
AVG(DATEDIFF( HOUR , ATC_DT_ATR , ATC_DT_DES )) AS 'Tempo atracado médio em horas', 
ATC_M_DT_INI_OP  AS 'Mes', 
ATC_A_DT_INI_OP AS 'ANO' 
from ATRACACAO_FATO
WHERE ATC_A_DT_INI_OP IN ('2020', '2021') AND ATC_CDTUP IN ('BRRN005', 'BRRN003', 'BRBA005', 'BRIQI', 'BRRN006', 'BRBA006', 'BRIOS', 'BRARB', 'BRARE', 'BRRN001', 'BRBA003', 'BRSE001', 'BRMCZ', 'BRMA001', 'BRSUA', 'BRRN004', 'BRCE001', 'BRBA009', 'BRBA004', 'BRCDO', 'BRREC', 'BRBA008', 'BRBA012', 'BRFOR', 'BRBA002', 'BRAL001', 'BRSSA', 'BRSE002', 'BRNAT', 'BRPE001', 'BRBA007',  'BRMA002')
AND ATC_DT_CHE IS NOT NULL AND ATC_DT_INIOP IS NOT NULL
GROUP BY ATC_CDTUP, ATC_CP, ATC_M_DT_INI_OP, ATC_A_DT_INI_OP
ORDER BY 4 ASC"""


#Query dados do Nordeste com a coluna Regiao no banco de dados
query_Nordeste_regiao = """select ATC_CP AS 'Localidade',
count(*) AS 'Número de Atracações', 
ATC_CDTUP, 
AVG(DATEDIFF( HOUR , ATC_DT_CHE , ATC_DT_INIOP )) AS 'Tempo de espera médio em horas', 
AVG(DATEDIFF( HOUR , ATC_DT_ATR , ATC_DT_DES )) AS 'Tempo atracado médio em horas', 
ATC_M_DT_INI_OP  AS 'Mes', 
ATC_A_DT_INI_OP AS 'ANO' 
FROM  ATRACACAO_FATO 
WHERE ATC_A_DT_INI_OP IN ('2020', '2021') AND ATC_RGEO = 'Nordeste'
AND ATC_DT_CHE IS NOT NULL AND ATC_DT_INIOP IS NOT NULL
GROUP BY ATC_CDTUP, ATC_CP, ATC_M_DT_INI_OP, ATC_A_DT_INI_OP
ORDER BY 4 ASC
"""


#Query agrupado por Região do Brasil
query_Brasil_por_Regiao = """select ATC_RGEO AS 'Localidade',
count(*) AS 'Número de Atracações', 
AVG(DATEDIFF( HOUR , ATC_DT_CHE , ATC_DT_INIOP )) AS 'Tempo de espera médio em horas', 
AVG(DATEDIFF( HOUR , ATC_DT_ATR , ATC_DT_DES )) AS 'Tempo atracado médio em horas', 
ATC_M_DT_INI_OP  AS 'Mes', 
ATC_A_DT_INI_OP AS 'ANO' 
FROM ATRACACAO_FATO 
WHERE ATC_A_DT_INI_OP IN ('2020', '2021') AND ATC_DT_CHE IS NOT NULL AND ATC_DT_INIOP IS NOT NULL
GROUP BY ATC_RGEO, ATC_M_DT_INI_OP, ATC_A_DT_INI_OP
ORDER BY 1 ASC"""


#Query agrupado por Região do Brasil
query_variancia = """SELECT ATC_A_DT_INI_OP as ano,
ATC_M_DT_INI_OP as mes,
COUNT(ATC_A_DT_INI_OP) as 'num_de_atracacoes',
		COUNT(ATC_A_DT_INI_OP) - LAG(COUNT(ATC_A_DT_INI_OP)) OVER (ORDER BY ATC_A_DT_INI_OP ASC, ATC_M_DT_INI_OP ASC) as 'variancia'
		 FROM ATRACACAO_FATO
	WHERE ATC_A_DT_INI_OP IN ('2020', '2021')
GROUP BY ATC_A_DT_INI_OP, ATC_M_DT_INI_OP
ORDER BY ATC_A_DT_INI_OP DESC, len(ATC_M_DT_INI_OP) DESC, ATC_M_DT_INI_OP DESC
"""

planilha_ceara = pd.read_sql(query_ceara, cnxn)
planilha_regiao = pd.read_sql(query_Nordeste, cnxn)
planilha_brasil = pd.read_sql(query_Brasil_por_Regiao, cnxn)
variancia_a_m = pd.read_sql(query_variancia, cnxn)

try:
    #if path already exists, remove it before copying with copytree()
    if os.path.exists(zone_ouro):
        shutil.rmtree(zone_ouro)
        shutil.copytree(zone_prata, zone_ouro)
except OSError as e:
    # If the error was caused because the source wasn't a directory
#    else:
        print('Directory not copied. Error: %s' % e)

os.chdir(zone_ouro)
planilha_ceara.to_excel('planilha_ceara.xlsx' )
planilha_regiao.to_excel('planilha_regiao.xlsx')
planilha_brasil.to_excel('planilha_brasil.xlsx')
variancia_a_m.to_excel('variancia_a_m.xlsx')
