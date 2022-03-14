import socket
import websockets
import asyncio
import os
import time
import datetime
path="./message.txt"
async def send_msg(websocket):
     while True:
        print("send message")
        f=open(path,'r')
        data=f.read()
        await websocket.send(data)
        print(datetime.datetime.now().strftime('%H:%M:%S'))
        print(data)
        f.close()
        time.sleep(5)
        
# 服务器端主逻辑
async def main_logic(websocket, path):
    await send_msg(websocket)

start_server = websockets.serve(main_logic, '127.0.0.1', 50008)
print("start_server\n")
asyncio.get_event_loop().run_until_complete(start_server)
asyncio.get_event_loop().run_forever()


