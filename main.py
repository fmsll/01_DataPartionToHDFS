import copy
import subprocess
from DataSource import *


# FUNÇÃO PARA EXECUÇÃO DO COMANDO DOCKER 'EXEC' DENTRO DO CONTAINER
# Para o funcionamento desta função é necessário que o docker esteja com acesso root
# Para mais informações para dar acesso root ao docker acessar:
# https://docs.docker.com/engine/install/linux-postinstall/
def executa_comando_container(*args, container_name: str) -> str:
    argumentos_comando = ['docker', 'exec', container_name]
    for arg in args:
        argumentos_comando.append(arg)
    proc = subprocess.run(
        argumentos_comando,
        capture_output=True,
        text=True  # TRANSFORMA A SAIDA DO COMANDO EM TEXTO PARA RETORNAR STR
    )
    return proc.stdout


# FUNÇÃO PARA CÓPIA DOS ARQUIVOS PARA DENTRO DO CONTAINER
def copia_arquivos_para_container(src_path: str, dest_path: str, container_name: str):
    proc = subprocess.run(
        ['docker', 'cp', f'{src_path}', f'{container_name}:{dest_path}']
    )


# FUNÇÃO PARA CÓPIA DOS ARQUIVOS DE DENTRO DO CONTAINER PARA DENTRO DO HDFS
def copia_arquivos_para_hdfs(src_path: str, dest_path: str, container_name: str):
    proc = subprocess.run(
        ['docker', 'exec', container_name, 'hdfs', 'dfs', '-put', f'{src_path}', f'{dest_path}']
    )


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


def stdoutpu_tolist(string: str) -> []:
    tolist = string.split()
    return tolist


# ++++++++++++++++++ INICIO DO PROGRAMA ++++++++++++++++++
if __name__ == "__main__":

    # Variáveis Obrigatórias relacionadas aos arquivos fonte;
    # Formato variável source_path: "<subDir>/<subDir1/.../<subDirN>/;

    source = None
    source_path = "data/"
    source_file_extension = ".csv"
    partition_field = "estado"

    # Variáveis Obrigatórias HDFS;

    hdfs_container_name = "namenode"
    hdfs_source_dir = "/user/covid19"

    # Variáveis Obrigatórias DOCKER;
    # 'container_dest_path' especifica o diretório dentro do container que serão copiados os arquivos fonte;

    container_dest_path = 'input/'

    # Verifica a existência do diretório informado como repositório dos arquivo(s) fonte;
    # Se existir inicializa o objeto DataSource;

    if os.path.isdir(source_path):
        source = DataSource(source_path, source_file_extension)
    else:
        print("Diretório de arquivos fonte não encontrado")
        exit()

    # Mapeamento dos arquivos fonte;
    # Caso não seja encontrado nenhum arquivo no diretório especificado a execução do programa é terminada;

    try:
        source.get_source_files()
        if not source.files_name == []:
            print("Arquivos Fonte Mapeados")
            print("-----------------------")
            for y, x in enumerate(source.files_name):
                print(f'{y+1}. {x}')
            print()
        else:
            print("Nenhum Arquivo Encontrado no diretório especificado")
            exit()
    except:
        print("Aconteceu algo errado")

    # Obtém o nome das partições que serão criadas;

    try:
        if not source.files_name == []:
            source.get_partitions_names(partition_field)
            print("Partições Mapeadas")
            print("------------------")
            print(source.partitions_name)
            print()
    except:
        print("Aconteceu algo errado")

    # Criação dos diretório base no HDFS;
    # Este comando procura pelo diretório especificado na variável 'hdfs_dest_dir' dentro do HDFS;

    executa_comando_container('hdfs', 'dfs', '-mkdir', hdfs_source_dir, container_name=hdfs_container_name)

    # Verificação se o diretório foi criado com sucesso;
    # WARNING: Este comando busca o diretório base dentro do diretório '/user' do HDFS;
    # Caso não seja encontrado o diretório a execução do programa é terminada;
    print("Criando diretório base dentro do HDFS")
    print("-------------------------------------")
    saida = executa_comando_container('hdfs', 'dfs', '-ls', '/user', container_name=hdfs_container_name)
    saida_list = stdoutpu_tolist(saida)
    if hdfs_source_dir in saida_list:
        print("Diretório base criado com sucesso")
        print()
    else:
        print("Diretório não foi criado")
        exit()

    # Particionamento dos arquivos fonte de acordo com o campo escolhido para particionamento;
    # Arquivos serão particionados no diretório especificado na variável 'source_path';
    # Será criado um arquivo com a terminação 'null' onde serão colocados todos os registros que tiverem o campo
    # especificado para particionamento com valor vazio;

    source.split_data_in_partitions()
    print()

    # Cópia dos arquivos particionados do sistema local para dentro do container

    nome_particoes = list.copy(source.partitions_name)
    print("Iniciando cópia dos arquivos do sistema local para dentro do container", hdfs_container_name.upper())
    print("----------------------------------------------------------------------", '-'*len(hdfs_container_name))
    while not nome_particoes == []:
        nome_particao = nome_particoes.pop()
        if not nome_particao == '':
            file = nome_particao + ".csv"
            copia_arquivos_para_container(f'{source_path}/{file}', f'{container_dest_path}',
                                          container_name=hdfs_container_name)
            print(f"Arquivo: {file} OK")
    print()
    # Criação dos diretórios do particionamento dentro do HDFS;

    particoes = list.copy(source.partitions_name)
    print("Iniciando criação dos diretórios das partições para dentro do HDFS")
    print("------------------------------------------------------------------")
    for num_particioes ,nome_particao in enumerate(particoes):
        executa_comando_container('hdfs', 'dfs', '-mkdir', f'{hdfs_source_dir}/{particoes[num_particioes]}',
                                  container_name=hdfs_container_name)
        print(f'Diretório {hdfs_source_dir}/{particoes[num_particioes]} OK')
    print()

    # Cópia dos arquivos particionados de dentro do conteiner do HADOOP para dentro HDFS
    #
    nome_particioes = list.copy(source.partitions_name)
    print("Iniciado copia dos arquivos para dentro do HDFS")
    print("-----------------------------------------------")
    while not nome_particioes == []:
        nome_particao = nome_particioes.pop()
        if not nome_particao == '':
            file = nome_particao + ".csv"
            copia_arquivos_para_hdfs(f'{container_dest_path}/{file}', f'{hdfs_source_dir}/{nome_particao}',
                                     container_name=hdfs_container_name)
            print(f'Arquivo {file} ')
        else:
            pass

