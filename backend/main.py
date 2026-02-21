from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

from routers import auth, bot_config, orders

load_dotenv()

app = FastAPI(title="Chel3D API", description="API для заявок Chel3D")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router, prefix="/api/auth", tags=["auth"])
app.include_router(orders.router, prefix="/api/orders", tags=["orders"])
app.include_router(bot_config.router, prefix="/api/bot-config", tags=["bot-config"])


@app.get("/")
async def root():
    return {"message": "Chel3D API"}


@app.get("/health")
async def health_check():
    return {"status": "healthy"}
