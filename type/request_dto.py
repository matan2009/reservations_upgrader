from pydantic.main import BaseModel


class RequestDTO(BaseModel):
    reservation_id: str
