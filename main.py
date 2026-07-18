import os
import math
from fastapi import FastAPI, HTTPException, Request, Response, Form, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from telethon import TelegramClient
from telethon.sessions import StringSession

# ==========================================
# 1. 環境變數設定 (已對齊 Render 後台設定)
# ==========================================
API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("SESSION_STRING", "")

# 根據 Render 後台設定，公開群組變數名為 TARGET_GROUP_ID
PUBLIC_CHAT_ID = int(os.environ.get("TARGET_GROUP_ID", "0"))
SECRET_CHAT_ID = int(os.environ.get("SECRET_TELEGRAM_CHAT_ID", "0"))

# ==========================================
# 2. FastAPI 與 CORS 初始化
# ==========================================
app = FastAPI(title="Meowtube API")

# 允許所有來源連線，確保 Nuxt 前端不會遇到 CORS 阻擋
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 初始化 Telethon 客戶端 (使用 StringSession 一般使用者登入)
client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

@app.on_event("startup")
async def startup_event():
    # 伺服器啟動時，啟動 Telegram 連線 (無需 bot_token)
    await client.start()

# ==========================================
# 3. 防休眠與喚醒端點 (Cron-job.org 專用)
# ==========================================
@app.get("/ping")
def ping():
    return {"status": "awake", "message": "Meowtube API is perfectly running!"}

# ==========================================
# 4. 上傳影片端點 (支援私密群組分流)
# ==========================================
@app.post("/upload")
async def upload_video(
    file: UploadFile = File(...),
    title: str = Form(...),
    is_secret: bool = Form(False)  # 接收前端的私密勾選狀態，預設為 False
):
    # 根據 is_secret 決定要上傳到哪個群組
    target_chat_id = SECRET_CHAT_ID if is_secret else PUBLIC_CHAT_ID
    
    # 暫存檔案至 Render 的 /tmp 資料夾
    temp_path = f"/tmp/{file.filename}"
    with open(temp_path, "wb") as buffer:
        buffer.write(await file.read())
        
    try:
        # 透過 Telethon 傳送檔案到指定的 Telegram 群組
        msg = await client.send_file(target_chat_id, temp_path, caption=title)
        
        # 上傳完成後，刪除暫存檔釋放空間
        if os.path.exists(temp_path):
            os.remove(temp_path)
            
        return {
            "message": "上傳成功",
            "is_secret": is_secret,
            "chat_id": target_chat_id,
            "tg_message_id": msg.id
        }
        
    except Exception as e:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        raise HTTPException(status_code=500, detail=f"上傳 Telegram 失敗: {str(e)}")

# ==========================================
# 5. 影片串流端點 (加入 is_secret 參數防止 ID 衝突)
# ==========================================
@app.get("/stream/{message_id}")
async def stream_video(message_id: int, request: Request, is_secret: bool = False):
    # 直接根據前端傳來的參數鎖定群組，不再兩邊瞎找
    target_chat_id = SECRET_CHAT_ID if is_secret else PUBLIC_CHAT_ID
    
    try:
        message = await client.get_messages(target_chat_id, ids=message_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Telegram 讀取失敗: {str(e)}")
        
    # 確認訊息存在且含有媒體檔案 (影片)
    if not message or not message.media:
         raise HTTPException(status_code=404, detail="找不到影片，或該訊息不含媒體檔案")
         
    file_size = message.document.size
    
    # 處理 HTTP Range Requests (這段是能讓影片可以隨意快進、拖曳的關鍵)
    range_header = request.headers.get("Range")
    if range_header:
        start, end = range_header.replace("bytes=", "").split("-")
        start = int(start)
        end = int(end) if end else file_size - 1
    else:
        start = 0
        end = file_size - 1
        
    chunk_size = end - start + 1
    
    # 建立非同步產生器，從 Telegram 抓取檔案碎片並即時回傳給前端
    async def video_generator():
        async for chunk in client.iter_download(message.media, offset=start, limit=chunk_size):
            yield chunk

    # 設定正確的回傳標頭
    headers = {
        "Content-Range": f"bytes {start}-{end}/{file_size}",
        "Accept-Ranges": "bytes",
        "Content-Length": str(chunk_size),
        "Content-Type": "video/mp4",
    }
    
    status_code = 206 if range_header else 200
    return StreamingResponse(video_generator(), status_code=status_code, headers=headers)
