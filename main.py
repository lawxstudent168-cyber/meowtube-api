import os
import re
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from telethon import TelegramClient
from telethon.sessions import StringSession

app = FastAPI(title="家庭劇院串流 API")

# 允許前端跨網域存取
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 載入環境變數
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("SESSION_STRING", "")
TARGET_GROUP_ID = int(os.environ.get("TARGET_GROUP_ID", 0))

# 初始化 Telegram 客戶端
client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

@app.on_event("startup")
async def startup_event():
    """伺服器啟動時，自動連接 Telegram"""
    await client.connect()

@app.get("/")
def read_root():
    return {"status": "Home Theater Streaming API is awake and running!"}

# ==========================================
# ⬇️ 影片網頁串流播放端點 (精準 HTTP 206 斷點續傳)
# ==========================================
@app.get("/stream/{message_id}")
async def stream_movie(request: Request, message_id: int):
    # 確保連線活著
    if not client.is_connected():
        await client.connect()

    # 取得影片訊息
    message = await client.get_messages(TARGET_GROUP_ID, ids=message_id)
    if not message or not getattr(message, 'file', None):
        raise HTTPException(status_code=404, detail="在 Telegram 中找不到此影片檔")

    file_size = message.file.size
    range_header = request.headers.get("Range")
    
    start = 0
    end = file_size - 1
    status_code = 200

    # 1. 解析瀏覽器要求的 Range (例如 bytes=100-200)
    if range_header:
        range_match = re.match(r"bytes=(\d+)-(\d*)", range_header)
        if range_match:
            start = int(range_match.group(1))
            end_str = range_match.group(2)
            if end_str:
                end = int(end_str)
            status_code = 206 # 標記為 206 Partial Content，允許跳轉與斷點續傳

    # 範圍錯誤保護
    if start >= file_size:
        return Response(
            status_code=416,
            headers={"Content-Range": f"bytes */{file_size}"}
        )

    # 這次請求總共需要傳輸多少位元組
    req_length = end - start + 1

    # 2. 核心邏輯：Telegram 規定 offset 必須嚴格對齊 (我們以 1MB 為單位對齊)
    CHUNK_SIZE = 1024 * 1024
    aligned_start = start - (start % CHUNK_SIZE)
    skip_bytes = start - aligned_start # 計算從對齊點開始，有多少位元組是我們「不需要」的

    async def video_streamer():
        yielded_bytes = 0
        skip = skip_bytes
        
        # 3. 正確使用 request_size，並從對齊點 (aligned_start) 開始抓取
        async for chunk in client.iter_download(
            message.media,
            offset=aligned_start,
            request_size=CHUNK_SIZE
        ):
            # 💡 致命錯誤修復：將 Telegram 的 memoryview 強制轉型為 FastAPI 認得的 bytes
            chunk_data = bytes(chunk)

            # 如果需要跳過前面的無用位元組 (這通常發生在第一包 chunk)
            if skip > 0:
                chunk_data = chunk_data[skip:]
                skip = 0
                
            # 如果加上這塊 chunk 會超過瀏覽器要求的總長度，就精準切斷並結束
            if yielded_bytes + len(chunk_data) > req_length:
                chunk_data = chunk_data[:req_length - yielded_bytes]
                yield chunk_data
                break
                
            yield chunk_data
            yielded_bytes += len(chunk_data)

    # 4. 嚴格遵守 HTML5 影片串流的 Header 規範
    headers = {
        "Accept-Ranges": "bytes",
        "Content-Length": str(req_length),
        "Content-Type": message.file.mime_type or "video/mp4",
    }
    
    if status_code == 206:
        headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"

    return StreamingResponse(
        video_streamer(), 
        status_code=status_code,
        headers=headers,
        media_type=message.file.mime_type or "video/mp4"
    )
