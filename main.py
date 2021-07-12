import subprocess


# FUNÇÃO EXECUTA COMANDO CONTAINER

def executa_comando_container(*args, container_name: str) -> str:
    argumentos_comando = ['sudo', 'docker', 'exec', container_name]
    for x in args:
        argumentos_comando.append(x)
    proc = subprocess.run(
        argumentos_comando,
        capture_output=True,
        text=True  # TRANSFORMA A SAIDA DO COMANDO EM TEXTO PARA RETORNAR STR
    )
    return proc.stdout


# FUNÇÃO PARA VERIFICAR A EXISTÊNCIA DO BANCO DE DADOS NO HIVE

def verifica_banco_dados_existe(container_name: str, dbname: str) -> bool:
    saida = str(executa_comando_container('hive', '-e', "show databases", container_name=container_name)).split()
    if dbname in saida:
        return True
    else:
        return False


# FUNÇÃO PARA LISTAR DATABASES NO HIVE

def lista_banco_de_dados(container_name:str):
    saida = str(executa_comando_container('hive', '-e', "show databases", container_name=container_name))
    return saida


# FUNÇÃO PARA LISTAR TABELAS NO HIVE

def lista_tabelas(container_name: str, dbname:str) -> str:
    saida = str(executa_comando_container('hive', '-e', f'use {dbname}; show tables', container_name=container_name))
    return saida


# FUNÇÃO PARA DAR APAGAR BANCO DE DADOS

def drop_database(container_name: str, dbname: str) -> bool:
    pass



#print(verifica_banco_dados_existe('hive-server', 'xxxx'))
print(lista_banco_de_dados(container_name="hive-server"))
print(lista_tabelas(container_name="hive-server",dbname="covid19"))
