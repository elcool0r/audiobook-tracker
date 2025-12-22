from passlib.context import CryptContext

PWD_CTX = CryptContext(schemes=["pbkdf2_sha256", "bcrypt"], deprecated="auto")


def verify_password(plain: str, hashed: str) -> bool:
    return PWD_CTX.verify(plain, hashed)


def get_password_hash(password: str) -> str:
    return PWD_CTX.hash(password)
