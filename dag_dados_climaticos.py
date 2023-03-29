from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import requests
import json
import pandas as pd
from datetime import datetime, timedelta

with DAG(
    "dados_climaticos", 
    start_date=datetime(2023, 3, 1),
    schedule_interval=timedelta(days=5),
    catchup=False,
    ) as dag:

    tarefa_1 = BashOperator(
        task_id = 'cria_pasta',
        bash_command = 'mkdir -p "/opt/airflow/dags/dados"'
    )
    def extrai_dados():
        API_KEY = "cc8f664af1186221fc1dd044fa96da2f"
        cidade = "santarem"
        link = f"https://api.openweathermap.org/data/2.5/forecast?q={cidade}&appid={API_KEY}&lang=pt_br"

        requisicao = requests.get(link)
        requisicao_dic = requisicao.json()
        dado = requisicao_dic['list']

        dados_list = [] #variavel para armazenar os dados que queremos em uma lista
        for dict_item in dado: #laço for para percorrer todas as informações contidas na variavel dados
            temp_min = dict_item['main']['temp_min'] #armazenando as informações de temperatura minima 
            temp_max = dict_item['main']['temp_max']  #armazenando as informações de temperatura maxima 
            desc = dict_item['weather'][0]['description'] #armazenando as informações de descrição 
            data = dict_item["dt_txt"] #armazenando as informações de datas e horarios
            wind = dict_item["wind"] #armazenando as informações de vento
            wind['Temp_min'] = temp_min #atualizando o dicionario wind e adicionando as informações de temperatura minima
            wind['Temp_max'] = temp_max ##atualizando o dicionario wind e adicionando as informações de temperatura maxima
            wind['Descricao'] = desc #atualizando o dicionario wind e adicionando as informações de descrição
            wind['Data'] = data #atualizando o dicionario wind e adicionando as informações de data e horario
            dados_list.append(wind) #incrementando os dados de wind na lista vazia dados_list
        
        dados_df = dict(dados_list[0])

        dados_df = pd.DataFrame(dados_list, columns=['speed','deg','gust','Temp_min','Temp_max','Descricao','Data'])
        dados_df = dados_df.rename(columns={'speed':'Vel_Km/h','deg':'Direcao','gust':'Rajada_Km/h','Temp_min':'Temp_min','Temp_max':'Temp_max','Descricao':'Descricao'})

        dados_df['Vel_Km/h'] = dados_df['Vel_Km/h'] * 3.6 #Convertendo de metro por segundo para km por hora
        dados_df['Vel_Km/h'] = dados_df['Vel_Km/h'].round() #arredondando os valores 
        dados_df['Rajada_Km/h'] = dados_df['Rajada_Km/h'] * 3.6 #Convertendo de metro por segundo para km por hora
        dados_df['Rajada_Km/h'] = dados_df['Rajada_Km/h'].round() #arredondando os valores

        dados_df['Data'] = dados_df['Data'].str[:-3] #ignorando as informações de data que nao queremos Ex De: 2023-03-27 21:00:00 Para: 2023-03-27 21:00
        dados_df['Data'] = dados_df['Data'].str.replace(':', 'H') #De: 2023-03-27 21:00:00 Para: 2023-03-27 21H00
        dados_df['Data'] = dados_df['Data'].str.replace('-', '/') #De: 2023-03-27 21H00 Para: 2023/03/27 21H00

        dados_df['Temp_min'] = dados_df['Temp_min'] - 273.15 #convertendo de kelvin para graus celsius
        dados_df['Temp_max'] = dados_df['Temp_max'] - 273.15 #convertendo de kelvin para graus celsius
        dados_df['Temp_min'] = dados_df['Temp_min'].astype(int) #convertendo float para inteiro
        dados_df['Temp_min'] = dados_df['Temp_min'].astype(str) + '°C' #convertendo em string e adicionando grau celsius
        dados_df['Temp_max'] = dados_df['Temp_max'].astype(int) #convertendo float para inteiro
        dados_df['Temp_max'] = dados_df['Temp_max'].astype(str) + '°C'#convertendo em string e adicionando grau celsius

        pd.set_option('display.expand_frame_repr', False) #expandindo dataframe para nao quebrar linhas

        dados_df.to_csv("/opt/airflow/dags/dados/arquivo-atualizado.csv" ,index=False)




    tarefa_2 = PythonOperator(
        task_id = 'extrai_dados',
        python_callable = extrai_dados
    )

    tarefa_1 >> tarefa_2