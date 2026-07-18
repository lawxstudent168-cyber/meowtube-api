import os
import math
from fastapi import FastAPI, HTTPException, Request, Response, Form, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from telethon import TelegramClient
from telethon.sessions import StringSession  # 💡 新增這行：匯入 StringSession 模組

# ==========================================
# 1. 環境變數設定 (對齊 Render 後台)
# ==========================================
API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "")
# 💡 改為讀取 SESSION_STRING
SESSION_STRING = os.environ.get("SESSION_STRING", "")

# 💡 根據截圖，將公開群組變數改為 TARGET_GROUP_ID
PUBLIC_CHAT_ID = int(os.environ.get("TARGET_GROUP_ID", "0"))
SECRET_CHAT_ID = int(os.environ.get("SECRET_TELEGRAM_CHAT_ID", "0"))

# ==========================================
# 2. FastAPI 與 CORS 初始化
# ==========================================
app = FastAPI(title="Meowtube API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 💡 初始化：改用 StringSession(SESSION_STRING) 作為登入憑證
client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

@app.on_event("startup")
async def startup_event():
    # 💡 啟動連線：因為 StringSession 已經包含了授權資訊，直接呼叫 start 即可，不需要 bot_token
    await client.start()

# ==========================================
# 3. 防休眠與喚醒端點
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
    is_secret: bool = Form(False)
):
    target_chat_id = SECRET_CHAT_ID if is_secret else PUBLIC_CHAT_ID
    
    temp_path = f"/tmp/{file.filename}"
    with open(temp_path, "wb") as buffer:
        buffer.write(await file.read())
        
    try:
        msg = await client.send_file(target_chat_id, temp_path, caption=title)
        
        if os.path.exists(temp_path):
            os.remove(temp_path)
            
        return {
            "message": "上傳成功",
            "is_secret": is_secret,
            "chat_id": target_chat_id,
            "tg_message_id": msg.id  # 這裡就能順利吐出 ID 給前端了
        }
        
    except Exception as e:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        raise HTTPException(status_code=500, detail=f"上傳 Telegram 失敗: {str(e)}")

# ==========================================
# 5. 影片串流端點
# ==========================================
@app.get("/stream/{message_id}")
async def stream_video(message_id: int, request: Request):
    message = await client.get_messages(PUBLIC_CHAT_ID, ids=message_id)
    
    if not message or not message.media:
        message = await client.get_messages(SECRET_CHAT_ID, ids=message_id)
        
    if not message or not message.media:
         raise HTTPException(status_code=404, detail="Video not found in any Telegram groups")
         
    file_size = message.document.size
    
    range_header = request.headers.get("Range")
    if range_header:
        start, end = range_header.replace("bytes=", "").split("-")
        start = int(start)
        end = int(end) if end else file_size - 1
    else:
        start = 0
        end = file_size - 1
        
    chunk_size = end - start + 1
    
    async def video_generator():
        async for chunk in client.iter_download(message.media, offset=start, limit=chunk_size):
            yield chunk

    headers = {
        "Content-Range": f"bytes {start}-{end}/{file_size}",
        "Accept-Ranges": "bytes",
        "Content-Length": str(chunk_size),
        "Content-Type": "video/mp4",
    }
    
    status_code = 206 if range_header else 200
    return StreamingResponse(video_generator(), status_code=status_code, headers=headers)
