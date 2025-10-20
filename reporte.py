import pandas as pd
import gspread
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pickle
import os
import requests
import time
import json
import matplotlib.pyplot as plt
import tempfile
import base64
from PIL import Image
import numpy as np

SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
SPREADSHEET_ID = '1hoXYiyuArtbd2pxMECteTFSE75LdgvA2Vlb6gPpGJ-g'
NOME_ABA = 'Contagem'
INTERVALO = 'C:H'
WEBHOOK_URL = "https://openapi.seatalk.io/webhook/group/uqHQVMpAQkqG1YEwJH8ogQ"

def autenticar_google():
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            try:
                flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
            except FileNotFoundError:
                print("Erro: O arquivo credentials.json não foi encontrado.")
                return None
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    return creds

def obter_totais_por_fanout(spreadsheet_id, nome_aba, intervalo):
    try:
        creds = autenticar_google()
        if not creds:
            return "Erro de autenticação. Verifique as credenciais."
            
        cliente = gspread.authorize(creds)
        planilha = cliente.open_by_key(spreadsheet_id)
        aba = planilha.worksheet(nome_aba)
    except Exception as e:
        return f"Erro ao conectar com a planilha: {e}"

    try:
        dados = aba.get(intervalo)
    except gspread.exceptions.APIError as e:
        return f"Erro na API do Google Sheets: {e}"

    header_row_index = -1
    for i, row in enumerate(dados):
        if row and 'FANOUT' in row[0].strip().upper():
            header_row_index = i
            break
    
    if header_row_index == -1:
        return "Não foi possível encontrar a linha do cabeçalho 'FANOUT' no intervalo."

    headers = dados[header_row_index]
    data = dados[header_row_index + 1:]
    
    if not data:
        return "Nenhum dado encontrado após o cabeçalho."

    df = pd.DataFrame(data, columns=headers)
    df.columns = [col.strip() for col in df.columns]

    colunas_desejadas = ['FANOUT', 'PALLET/SCUTTLE', 'SACA', 'TOTAL', "Qtd's Pacotes", 'TO Packed']
    for col in colunas_desejadas:
        if col not in df.columns:
            return f"A coluna '{col}' não foi encontrada. Cabeçalhos lidos: {df.columns.tolist()}"
    
    df = df.dropna(subset=['FANOUT'])

    colunas_numericas = ['PALLET/SCUTTLE', 'SACA', 'TOTAL', "Qtd's Pacotes", 'TO Packed']
    for col in colunas_numericas:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    df = df[(df[colunas_numericas] != 0).any(axis=1)]

    df['FANOUT'] = df['FANOUT'].str.strip()
    ordem_fanout = df['FANOUT'].unique()
    df['FANOUT'] = pd.Categorical(df['FANOUT'], categories=ordem_fanout, ordered=True)
    df = df.sort_values('FANOUT').reset_index(drop=True)

    return df

def salvar_tabela_como_imagem(df, caminho):
    fig, ax = plt.subplots(figsize=(14, len(df) * 0.5 + 1.5))
    ax.axis('off')

    tabela = ax.table(
        cellText=df.values,
        colLabels=df.columns,
        loc='center',
        cellLoc='center',
        colLoc='center'
    )

    tabela.auto_set_font_size(False)
    tabela.set_fontsize(10)
    tabela.scale(1.2, 1.2)

    # Ajustar largura das colunas: primeira mais larga, outras mais finas
    for (row, col), cell in tabela.get_celld().items():
        if col == 0:
            cell.set_width(0.2)
        else:
            cell.set_width(0.1)

    # Cabeçalho laranja
    for col in range(len(df.columns)):
        cell = tabela[0, col]
        cell.set_facecolor('#FFA500')

    plt.tight_layout()
    plt.savefig(caminho, bbox_inches='tight', pad_inches=0, dpi=200)
    plt.close()

    # Abrir a imagem e recortar as bordas brancas
    imagem = Image.open(caminho)
    imagem_np = np.array(imagem)

    mask = np.any(imagem_np[:, :, :3] < 250, axis=2)
    coords = np.argwhere(mask)

    if coords.size:
        y0, x0 = coords.min(axis=0)
        y1, x1 = coords.max(axis=0) + 1
        imagem_cortada = imagem.crop((x0, y0, x1, y1))
        imagem_cortada.save(caminho)

def enviar_webhook_texto(mensagem):
    print("Enviando mensagem de texto ao webhook...")
    try:
        payload = {
            "tag": "text",
            "text": {
                "format": 1,
                "content": mensagem
            }
        }
        response = requests.post(url=WEBHOOK_URL, json=payload)
        print(f"Status da resposta do Webhook: {response.status_code}")
        if response.text:
            print(f"Resposta do servidor: {response.text}")
        response.raise_for_status()
        print("Mensagem enviada com sucesso.")
    except Exception as e:
        print(f"Erro ao enviar mensagem: {e}")

def enviar_imagem_base64(caminho_imagem):
    print("Convertendo imagem para base64 e enviando ao SeaTalk...")
    try:
        with open(caminho_imagem, "rb") as img_file:
            img_base64 = base64.b64encode(img_file.read()).decode('utf-8')
        
        payload = {
            "tag": "image",
            "image_base64": {
                "content": img_base64
            }
        }
        response = requests.post(WEBHOOK_URL, json=payload)
        print(f"Status: {response.status_code}")
        if response.text:
            print("Resposta:", response.text)
        response.raise_for_status()
        print("Imagem enviada com sucesso.")
    except Exception as e:
        print(f"Erro ao enviar imagem: {e}")

if __name__ == "__main__":
    mensagem_inicial = "Segue o piso da expedição:"
    enviar_webhook_texto(mensagem_inicial)
    time.sleep(1)

    resultado = obter_totais_por_fanout(SPREADSHEET_ID, NOME_ABA, INTERVALO)

    if isinstance(resultado, pd.DataFrame):
        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as temp_img:
            salvar_tabela_como_imagem(resultado, temp_img.name)
            enviar_imagem_base64(temp_img.name)
            os.remove(temp_img.name)
    else:
        print("Erro ao obter dados:", resultado)
        enviar_webhook_texto(f"Erro ao gerar relatório:\n{resultado}")
