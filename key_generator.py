from cryptography.fernet import Fernet
import secrets

print(F"Your FERNET_KEY: {Fernet.generate_key().decode()}")
print(f"Your SECRET_KEY: {secrets.token_hex(32)}")