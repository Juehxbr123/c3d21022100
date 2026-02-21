import datetime

from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from pydantic import BaseModel

from config import settings

router = APIRouter()
security = HTTPBearer()
ALGORITHM = "HS256"


class LoginRequest(BaseModel):
    password: str


def create_access_token(data: dict):
    to_encode = data.copy()
    to_encode.update({"exp": datetime.datetime.utcnow() + datetime.timedelta(hours=24)})
    return jwt.encode(to_encode, settings.secret_key, algorithm=ALGORITHM)


def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        return jwt.decode(credentials.credentials, settings.secret_key, algorithms=[ALGORITHM])
    except JWTError as exc:
        raise HTTPException(status_code=401, detail="Недействительный токен") from exc


@router.post("/login")
async def login(request: LoginRequest):
    if request.password != settings.admin_panel_password:
        raise HTTPException(status_code=401, detail="Неверный пароль")
    return {"token": create_access_token({"sub": "admin"}), "token_type": "bearer"}


@router.get("/verify")
async def verify_token_endpoint(payload: dict = Depends(verify_token)):
    return {"valid": True, "user": payload.get("sub", "admin")}
