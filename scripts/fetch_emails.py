import os
import base64
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# Si modificas estos alcances, elimina el archivo token.json.
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

def get_gmail_service():
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return build('gmail', 'v1', credentials=creds)

def download_attachments(query, amex_folder, citi_folder):
    service = get_gmail_service()

    results = service.users().messages().list(userId='me', q=query).execute()
    messages = results.get('messages', [])

    if not messages:
        print(f"No se encontraron correos con la búsqueda: {query}")
        return

    for msg in messages:
        message = service.users().messages().get(
            userId='me', id=msg['id']
        ).execute()

        for part in message['payload'].get('parts', []):
            filename = part.get('filename')
            if not filename:
                continue

            filename_lower = filename.lower()
            if not filename_lower.endswith(('.csv', '.xlsx')):
                continue

            # --- Clasificación por banco ---
            if 'amex' in filename_lower or 'american' in filename_lower:
                target_folder = amex_folder
                bank = 'AMEX'
            else:
                target_folder = citi_folder
                bank = 'RAW'

            # --- Obtener datos ---
            if 'data' in part['body']:
                data = part['body']['data']
            else:
                att_id = part['body']['attachmentId']
                attachment = service.users().messages().attachments().get(
                    userId='me', messageId=msg['id'], id=att_id
                ).execute()
                data = attachment['data']

            file_data = base64.urlsafe_b64decode(data.encode('UTF-8'))
            path = os.path.join(target_folder, filename)

            with open(path, 'wb') as f:
                f.write(file_data)

            print(f"[{bank}] Descargado: {filename}")


if __name__ == "__main__":
    # Ejemplo de uso: Buscar archivos de AMEX de los últimos 7 días
    AMEX_FOLDER = "data/raw/unify_all_amex/"    
    CITI_FOLDER = "data/raw/"
    
    os.makedirs(AMEX_FOLDER, exist_ok=True)
    os.makedirs(CITI_FOLDER, exist_ok=True)
    
    # Puedes ajustar el query de Gmail (ejemplo: de un remitente específico)
    query = "{from:ricky@rentify.live from:lindsay@rentify.live from:lindsayareiter@gmail.com} amex has:attachment filename:(csv OR xlsx) after:2025/12/01"
    
    print(f"Buscando statements de los remitentes autorizados...")
    download_attachments(query, AMEX_FOLDER, CITI_FOLDER)