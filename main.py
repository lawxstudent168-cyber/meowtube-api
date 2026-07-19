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
        # 嘗試抓取
        message = await client.get_messages(target_chat_id, ids=int(message_id))
        
        # 防護機制：解決 Telethon 論壇模式快取問題
        if not message or not message.media:
            await client.get_dialogs()
            message = await client.get_messages(target_chat_id, ids=int(message_id))
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Telegram 讀取失敗: {str(e)}")
        
    if not message or not message.media:
         raise HTTPException(status_code=404, detail="找不到影片，或該訊息不含媒體檔案")
         
    # 👇 核心修復 1：使用 message.file 確保安全，並動態抓取真實的 mime_type
    file_size = message.file.size
# ❌ 刪除或註解掉這行：
    # mime_type = message.file.mime_type or "video/mp4"
    
    # ✅ 改成這行（強制所有檔案都用標準 mp4 格式串流給瀏覽器）：
    mime_type = "video/mp4"
    
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
        try:
            bytes_left = chunk_size
            
            # 👇 關鍵核心修改：將 message.media 改為直接傳入 message 本身！
            async for chunk in client.iter_download(message, offset=start):
                if bytes_left <= 0:
                    break
                if len(chunk) >= bytes_left:
                    yield chunk[:bytes_left]
                    break
                yield chunk
                bytes_left -= len(chunk)
                
        except Exception as e:
            # 加入這行，如果 Telegram 還是拒絕下載，Render 的 Logs 裡面就會印出真正原因
            print(f"影片串流發生致命錯誤: {str(e)}")

    # 👇 核心修復 2：嚴格遵守 HTTP 規範，區分 206 與 200 的 Headers 結構
    headers = {
        "Accept-Ranges": "bytes",
        "Content-Length": str(chunk_size),
        "Content-Type": mime_type, # 使用真實的檔案類型
    }
    
    if range_header:
        headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"
        status_code = 206
    else:
        status_code = 200
        
    return StreamingResponse(video_generator(), status_code=status_code, headers=headers)


# ==========================================
# 6. 專屬 Debug 探照燈 (用來檢測 Telegram 到底回傳了什麼)
# ==========================================
@app.get("/debug/{message_id}")
async def debug_telegram_message(message_id: int, is_secret: bool = False):
    target_chat_id = SECRET_CHAT_ID if is_secret else PUBLIC_CHAT_ID
    
    try:
        message = await client.get_messages(target_chat_id, ids=message_id)
        
        # 再次確保快取刷新
        if not message:
            await client.get_dialogs()
            message = await client.get_messages(target_chat_id, ids=message_id)
            
        if not message:
            return {"status": "failed", "detail": "Telegram 回報：完全找不到這則訊息"}
            
        return {
            "status": "success",
            "target_chat_id": target_chat_id,
            "message_id": message.id,
            "text": message.text,
            "has_media": bool(message.media),
            "media_type": type(message.media).__name__ if message.media else "無媒體",
            "file_name": message.file.name if (message.media and message.file) else "未知",
            "file_size_bytes": message.file.size if (message.media and message.file) else 0,
        }
    except Exception as e:
        return {"status": "error", "error_message": str(e)}
