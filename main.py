import os
from fastapi import FastAPI, HTTPException, Request, Form, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from telethon import TelegramClient
from telethon.sessions import StringSession

# ==========================================
# 1. 環境變數設定
# ==========================================
API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("SESSION_STRING", "")

# 讀取雙群組 ID：公開群組與私密群組
PUBLIC_CHAT_ID = int(os.environ.get("TARGET_GROUP_ID", "0"))
SECRET_CHAT_ID = int(os.environ.get("SECRET_TELEGRAM_CHAT_ID", "0"))

# ==========================================
# 2. FastAPI 與 CORS 初始化
# ==========================================
app = FastAPI(title="Meowtube API")

# 設定 CORS，並暴露串流所需的 Headers
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    # 👇 新增：讓瀏覽器能正確讀取影片長度與切割資訊，解決轉圈問題
    expose_headers=["Content-Range", "Accept-Ranges", "Content-Length"],
)

# 使用一般使用者帳號 (StringSession) 登入
client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

@app.on_event("startup")
async def startup_event():
    await client.start()

# ==========================================
# 3. 防休眠與喚醒端點
# ==========================================
@app.get("/ping")
def ping():
    return {"status": "awake", "message": "Meowtube API is perfectly running!"}

# ==========================================
# 4. 上傳影片端點
# ==========================================
@app.post("/upload")
async def upload_video(
    file: UploadFile = File(...),
    title: str = Form(...),
    is_secret: bool = Form(False) # 透過布林值決定是否為私密影片
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
            "tg_message_id": msg.id
        }
    except Exception as e:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        raise HTTPException(status_code=500, detail=f"上傳 Telegram 失敗: {str(e)}")

# ==========================================
# 5. 影片串流端點 (修復轉圈問題的最終版)
# ==========================================
@app.get("/stream/{message_id}")
async def stream_video(message_id: int, request: Request, is_secret: bool = False):
    target_chat_id = SECRET_CHAT_ID if is_secret else PUBLIC_CHAT_ID
    
    try:
        message = await client.get_messages(target_chat_id, ids=message_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Telegram 讀取失敗: {str(e)}")
        
    if not message or not message.media:
         raise HTTPException(status_code=404, detail="找不到影片，或該訊息不含媒體檔案")
         
    file_size = message.document.size
    
    # 計算瀏覽器要求的影片區塊 (HTTP 206 Partial Content)
    range_header = request.headers.get("Range")
    if range_header:
        start, end = range_header.replace("bytes=", "").split("-")
        start = int(start)
        end = int(end) if end else file_size - 1
    else:
        start = 0
        end = file_size - 1
        
    chunk_size = end - start + 1
    
    # 👇 新增：精準切割位元組的產生器，不讓瀏覽器噎到
    async def video_generator():
        bytes_left = chunk_size
        async for chunk in client.iter_download(message.media, offset=start):
            if bytes_left <= 0:
                break
            
            # 如果 Telegram 給的碎片比我們需要的大，就精準切割
            if len(chunk) >= bytes_left:
                yield chunk[:bytes_left]
                break
            
            # 如果還沒達到需要的量，就整塊給出去，並扣除已給的額度
            yield chunk
            bytes_left -= len(chunk)

    # 組合回傳給瀏覽器的 Header 資訊
    headers = {
        "Content-Range": f"bytes {start}-{end}/{file_size}",
        "Accept-Ranges": "bytes",
        "Content-Length": str(chunk_size),
        "Content-Type": "video/mp4",
    }
    
    status_code = 206 if range_header else 200
    return StreamingResponse(video_generator(), status_code=status_code, headers=headers)
