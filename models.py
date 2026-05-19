from sqlalchemy import Column, Integer, String
from database import Base

class Sala(Base):
    __tablename__ = "salas"

    id = Column(Integer, primary_key=True, index=True)
    nombre = Column(String)
    capacidad = Column(Integer)

class Reserva(Base):
    __tablename__ = "reservas"

    id = Column(Integer, primary_key=True, index=True)
    sala_id = Column(Integer)
    usuario = Column(String)
    hora = Column(String)