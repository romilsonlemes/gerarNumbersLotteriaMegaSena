import random
import os
import psycopg2
import datetime
import json


def salvarRegistrosPostGreSQL(NumRegistro, numeros):
    hostnameDB = "192.168.1.13"
    PortDB = "5432"
    try:
        conn = psycopg2.connect(
            host=hostnameDB,
            port=PortDB,
            database="megasena",
            user="postgres",
            password="090577#Rom@09"
        )
        print("Conexão estabelecida com sucesso ao Banco de dados.")
        cur = conn.cursor()
        # Pegar a data e a hora
        datahoraNow = datetime.datetime.now()
        datahoraNowFormat = datahoraNow.strftime("%Y-%m-%d %H:%M:%S")
        #print(f'Data e Hora: {datahoraNowFormat}')

        registro = [
                    (numeros, datahoraNowFormat)
                  ]
        # Verificar se os numeros recebidos existem na base de dados
        numerosDoJogo = json.dumps(registro[0][0])
        dataHoraReg = registro[0][1]
        numerosDoJogo = str(numerosDoJogo)
        numerosDoJogo = numerosDoJogo.replace(" ", "")
        numerosDoJogo = numerosDoJogo.replace("[", "")
        numerosDoJogo = numerosDoJogo.replace("]", "")
        numerosDoJogo = numerosDoJogo
        #print(f'numerosDoJogo: {numerosDoJogo}')

        #Verificar se o registro já existe na tabela
        sqlConsulta = f"SELECT * FROM jogosmega WHERE numerosJogo = '{numerosDoJogo}'"
        #print(f"Consulta: {sqlConsulta}")

        cur.execute(sqlConsulta)
        result = cur.fetchone()

        if (not result): # Se não encontra o registro, Cadastro o novo registro
            # Se não Existe o Registro, incluir o novo registro
            cur.executemany("INSERT INTO jogosmega (numerosjogo, datahora) VALUES (%s, %s)", registro)
            conn.commit()
            conn.close()
            # Mostrar o registro
            # Pegar a data e a hora

            datahoraNowFor = datetime.datetime.now()
            datahoraNowFormatFor = datahoraNowFor.strftime("%Y-%m-%d %H:%M:%S")
            print(f'Cartela n. {ps} - {numerosGerados} - DataHora: {datahoraNowFormatFor}')
            return 'Sucesso'

        else:
            #Registro de Numeros foi Encontrado
            print(f'Os numeros a seguir foram encontrados na Base de dados: {numerosDoJogo}')

    except:
        print("Falha na conexão com o  Banco de dados.")
        return 'Falha'


def gerar_Number_MegaSena():
    numero_aleatorios = random.sample(range(1, 61), 6)
    return sorted(numero_aleatorios)

if __name__ == '__main__':
    qtdeCartelas = 200000
    ps=0
    for n in range(qtdeCartelas):
        # Salvar no PostGreSQL
        ps +=1
        numerosGerados = gerar_Number_MegaSena()
        status = salvarRegistrosPostGreSQL(ps, numerosGerados)

        if (status != "Sucesso"):
            print('Fim do processamento por falha de acesso a base de dados do PostGreSQL')
            break


