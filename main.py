
# IMPORTAÇÕES
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaFileUpload

from sqlalchemy import create_engine, pool
import pandas as pd
import matplotlib.pyplot as plt

from datetime import datetime
import os


def main():
    # AUTENTICAÇÃO POR MEIO DE ARQUIVO (CONTA DE SERVIÇO)
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive', 
            'https://www.googleapis.com/auth/documents']

    creds = service_account.Credentials.from_service_account_file('credentials.json', scopes=SCOPES)

    # CONSULTA AO BANCO E GERAÇÃO DE DF
    sql='''
    SELECT 
	name_0 as nome_pais,
    count(1) as n_focos
    FROM
        public.focos_operacao
    WHERE
        data_pas>='20220101'
        and continente_id=8
        and cod_sat='AQUA_M-T'
    group by name_0;
    '''

    BD_CONNECTION = os.getenv('BD_CONNECTION')
    ENGINE = create_engine(f'postgresql+psycopg2://{BD_CONNECTION}', poolclass=pool.NullPool)
    df = pd.read_sql(sql, ENGINE)
    df_list = df.values.tolist()

    # GERA O GRÁFICO
    fig = plt.figure(figsize=(12, 3.15))
    ax = fig.add_axes([0,0,1,1])

    x_label = df.nome_pais
    ax.yaxis.grid(zorder=0)
    ax.bar(x_label, df['n_focos'], color='#f7cb4d', width=0.7, zorder=2)

    plt.xlabel('Países')
    plt.ylabel('N° Focos')
    plt.xticks(x_label)

    ax.get_figure().savefig(f"grafico_gerado.png", format='png', bbox_inches = "tight")


    # SERVICES DAS FERRAMENTAS GOOGLE
    service_sheet = build('sheets', 'v4', credentials=creds)
    service_drive = build('drive', 'v3', credentials=creds)
    service_docs  = build('docs', 'v1', credentials=creds)

    date_now = datetime.now().strftime("%d/%m/%Y %H:%M")

    # ENVIAR A IMAGEM PRO DRIVE
    service_drive.files().list(q="mimeType='application/vnd.google-apps.folder' and name='Webinar-Automacao-Google'", 
                               fields='files(id)').execute()
    output_folder = service_drive.files().list(q="mimeType='application/vnd.google-apps.folder' and name='saida'", 
                               fields='files(id)',).execute()
    img_folder_id = output_folder['files'][0]['id']

    media = MediaFileUpload('grafico_gerado.png', mimetype='image/png', resumable=True)
    service_drive.files().create(body={'name': 'grafico_gerado.png', 'parents': [img_folder_id]}, 
                                 media_body=media, 
                                 fields='id').execute()
    print (f'Subindo a imagem para o drive!')

    # ATUALIZA A PLANILHA
    id_planilha = '17zwhbAIyMv92N8wnueMx8N9fcFypN2kXj48cGaw5Cqc'

    service_sheet.spreadsheets().values().update(
        spreadsheetId=id_planilha,
        range='Página1!A1', valueInputOption="RAW",
        body={"values": df_list}
        ).execute()
    service_sheet.spreadsheets().values().update(
        spreadsheetId=id_planilha,
        range='Página1!D5', valueInputOption="RAW",
        body={"values": [['Atualizado em: '],[date_now]]}
        ).execute()


    # CRIA DOCUMENTO USANDO O BASE
    doc_base = service_drive.files().list(q="name='DOC BASE'", spaces='drive', fields='files(id)').execute()
    doc_id_base = doc_base['files'][0]['id']
    body = {'name': 'DOC NOVO - ' + date_now}
    response = service_drive.files().copy(fileId=doc_id_base, body=body).execute()
    doc_id = response.get('id')


    #ATUALIZA OS TEXTOS DO DOCS 
    texts = {
        'data_doc': date_now,
        'num_max_focos': str(df['n_focos'].max()),
        'pais_max_focos': df['nome_pais'][df['n_focos'] == df['n_focos'].max()].values[0]
        }
    requests_text=[]
    for key in texts.keys():
        if texts[key] != '' and texts[key] is not None:
            requests_text.append({
                                'replaceAllText': {
                                    'containsText': {
                                        'text': '{{'+key+'}}',
                                        'matchCase':  'true'
                                    },
                                    'replaceText': texts[key],
                                }
                            })
    service_docs.documents().batchUpdate(documentId=doc_id, body={'requests': requests_text}).execute()
    print('Textos alterados!')


    # ATUALIZA AS IMAGENS DO DOCS

    # Método para pegar o id "local" da imagem no documento base (campo 'objectId', inicia com "kix")
    # print( id_imagens = service_docs.documents().get(documentId=doc_id, fields='inlineObjects').execute() )

    # {'nome_da_imagem_no_drive.png': "id_imagem_doc_base"}
    objects = {'grafico_gerado.png': 'kix.71znzotqim9j'}

    requests_img = []
    images = service_drive.files().list(q="mimeType='image/png' and '"+img_folder_id+"' in parents", 
                                        spaces='drive',
                                        fields='files(id, name, webContentLink)',).execute()
    for img in images['files']:  
        if img['name'] in objects.keys():
            print('Substituindo a imagem ' + img['name'])
            service_drive.permissions().create(body={"role":"reader", "type":"anyone"}, fileId=img['id']).execute()
            requests_img = [{
                'replaceImage': {
                    'imageObjectId': objects[img['name']],
                    'uri': img['webContentLink']
                },
            }]
            service_docs.documents().batchUpdate(documentId=doc_id, body={'requests': requests_img}).execute()
    print('Imagens alteradas!')

# Executa o código todo pelo terminal: "python3 main.py"
if __name__ == '__main__':
    main()