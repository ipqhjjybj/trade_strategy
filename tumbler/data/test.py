# coding=utf-8

import json
import zlib

# run by python3
import asyncio
import websockets


async def get_info():
	url = "wss://api.huobi.pro/ws"
	async with websockets.connect(url) as ws:
		# req1 = """
		# 	{
		# 	  "sub": "market.ethbtc.kline.4hour",
		# 	  "id": "id1"
		# 	}
		# """
		
		# await ws.send(req1)
		# msg_recv = await ws.recv()
		# msg_recv = json.loads(zlib.decompress(msg_recv, 31))
		# print(msg_recv)

		# a = await ws.recv()
		# print(json.loads(zlib.decompress(a, 31)))

		msg = """{
			"req": "market.ethbtc.kline.4hour",
			"id": "id1",
			"from": 1589836400,
			"to": 1590503841
		}"""
		print(msg)

		await ws.send(msg)
		msg_recv = await ws.recv()
		msg_recv = json.loads(zlib.decompress(msg_recv, 31))
		print(msg_recv)
		return msg_recv


data = asyncio.get_event_loop().run_until_complete(get_info())
print(data)
