import fastapi
from .sensors.controller import router as sensorsRouter
import yoyo

app = fastapi.FastAPI(title="Senser", version="0.1.0-alpha.1")

app.include_router(sensorsRouter)

backend = yoyo.get_backend("postgresql://timescale:timescale@timescale:5433/timescale")
migrations = yoyo.read_migrations("migrations_ts")

with backend.lock():
    backend.apply_migrations(backend.to_apply(migrations))

@app.get("/")
def index():
    #Return the api name and version
    return {"name": app.title, "version": app.version}
