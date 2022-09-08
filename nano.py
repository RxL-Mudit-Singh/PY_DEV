from typing import Union
from fastapi import FastAPI, HTTPException, Response,status
from pydantic import BaseModel
from pytest import Item
import json
import uvicorn
app = FastAPI()


students=[
    {'name':'mudit','age':22,'id':1831034},
    {'name':'anvesh','age':21,'id':1831035},
    {'name':'aditya','age':20,'id':1831036},
    {'name':'pranjal','age':24,'id':1831037},
    {'name':'aayush','age':21,'id':1831038}
]

Users = {
    "X1":{
        "id":90,
        "first_name":"Mudit",
        "email":"xyz@gmail.com"
    },
    "X2":{
        "id":91,
        "first_name":"Anvesh",
        "email":"xyz2@gmail.com"
    },
    "X3":{
        "id":92,
        "first_name":"Aditya",
        "email":"xyz3@gmail.com"
    }
}

class items(BaseModel):
    name: str
    desc: Union[str, None] = None
    price: float
    tax: Union[float,None] = None

@app.get("/")
async def root():
    return {"HTTp"}

@app.post("/items")
def create_item(item:items):
    return item

@app.get('/students')
def user_list():
    return {'students':students}

def student_check(id):
    if not students[id]:
        raise HTTPException(status_code=404,detail='Students Not Found')

@app.get('/students/{student_id}')
def get_details(student_id:int):
    student_check(student_id)
    return {'student':students[student_id]}


@app.get("/user/{username}")
async def get_details(username,response:Response):
    try:
        result = Users[username.upper()]
    except KeyError:
        result = {"err":f"No Such user: {username}"}
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    return result


if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8001)