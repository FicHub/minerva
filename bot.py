#!/usr/bin/env python3
import json
import traceback
import urllib.parse
import datetime
from bs4 import BeautifulSoup
import discord
import asyncio
from oil import oil

client = discord.Client()
db = oil.open()

API_PREFIX = 'https://fic.pw/api/v0/epub?automated=true&q='

def lookup(query: str):
	import requests
	try:
		req = requests.get(API_PREFIX + query)
		res = req.json()
		return res
	except:
		return {'error':-1,'msg':'lookup failed :('}

class RequestLog:
	def __init__(self, id_, created_, infoRequestMs_, epubCreationMs_, urlId_,
			query_, ficInfo_, epubFileName_, hash_, url_, isAutomated_):
		self.id = id_
		self.created = created_
		self.infoRequestMs = infoRequestMs_
		self.epubCreationMs = epubCreationMs_
		self.urlId = urlId_
		self.query = query_
		self.ficInfo = ficInfo_
		self.epubFileName = epubFileName_
		self.hash = hash_
		self.url = url_
		self.isAutomated = isAutomated_

	@staticmethod
	def maxId() -> int:
		with db.cursor() as curs:
			curs.execute('select max(id) from requestLog')
			r = curs.fetchone()
			return r[0]
		return -1

	@staticmethod
	def fetchAfter(after):
		with db.cursor() as curs:
			curs.execute('select * from requestLog where id > %s', (after,))
			ls = [RequestLog(*r) for r in curs.fetchall()]
			return ls
		return []

	@staticmethod
	def mostRecentByUrlId(urlId):
		with db.cursor() as curs:
			curs.execute('''
			select * from requestLog
			where urlId = %s
			order by created desc limit 1''', (urlId,))
			r = curs.fetchone()
			if r is None:
				return None
			return RequestLog(*r)

@client.event
async def on_ready():
	print('We have logged in as {0.user}'.format(client))

async def sendFicInfo(channel, l):
	try:
		url = urllib.parse.urljoin('https://fic.pw/', l.url)
		info = json.loads(l.ficInfo)
		descSoup = BeautifulSoup(info['desc'], 'lxml')
		infoTime = f'{l.infoRequestMs/1000.0:.3f}s'
		epubTime = f'{l.epubCreationMs/1000.0:.3f}s'
		msg = f'request for <{l.query}> => `{l.urlId}` ({infoTime})'
		msg += f', generated epub in {epubTime}'
		title = f'{info["title"]} by {info["author"]}'
		desc = '\n'.join([
				f'{descSoup.get_text()}',
				f'{info["words"]} words in {info["chapters"]} chapters',
			])
		e = discord.Embed(title=title, description=desc, url=url)
		await channel.send(msg, embed=e)
		return True
	except Exception as e:
		traceback.print_exc()
		print(e)
		print('sendFicInfo: error: ^')
	return False

@client.event
async def on_message(message):
	print(json.dumps({"message":{
		"author":str(message.author),
		"content":str(message.content),
	}}))
	if message.author == client.user:
		return

	if message.content.startswith('!test'):
		await message.channel.send('Hello!')
	infoCommandPrefixes = ['lookup', 'info', 'epub', 'link']
	for pre in infoCommandPrefixes:
		if message.content.startswith(f"!{pre}"):
			query = message.content[len(f"!{pre}"):].strip()
			break
	else:
		return
	query = query.strip('<>| \t').strip()
	await message.add_reaction('👍')

	try:
		fut = asyncio.get_event_loop().run_in_executor(None, lookup, query)
		res = await fut
	except Exception as e:
		await message.channel.send(f"unable to lookup '{query}'")
		await message.add_reaction('❌')
		traceback.print_exc()
		print(e)
		print('error: ^')

	# FIXME why do we need this client param?
	await message.remove_reaction('👍', client.user)
	if res is None or 'urlId' not in res \
			or ('error' in res and int(res['error']) != 0):
		await message.channel.send(f"unable to lookup '{query}'")
		await message.add_reaction('❌')
		print(res)
		return

	try:
		l = RequestLog.mostRecentByUrlId(res['urlId'])
		await sendFicInfo(message.channel, l)
		await message.add_reaction('✅')
	except Exception as e:
		await message.channel.send(f"unable to find lookup result for '{query}'")
		await message.add_reaction('❌')
		traceback.print_exc()
		print(e)
		print('error: ^')

def pruneRecent(recent, minutes=15):
	nr = {}
	old = (datetime.datetime.now() - datetime.timedelta(minutes=minutes))
	for key in recent:
		if recent[key] < old:
			continue
		nr[key] = recent[key]
	return nr

async def watch_requests():
	await client.wait_until_ready()
	botspam_priv = client.get_channel(754481638695501866) # #botspam-priv
	request_feed = client.get_channel(754779814740754492) # #request-feed
	maxId = RequestLog.maxId()
	recentHashes = {}
	await botspam_priv.send('started up')
	while not client.is_closed():
		recentHashes = pruneRecent(recentHashes)

		await asyncio.sleep(3)
		ls = RequestLog.fetchAfter(maxId)
		for l in ls:
			maxId = max(maxId, l.id)
			if l.hash in recentHashes:
				continue
			recentHashes[l.hash] = l.created

			url = urllib.parse.urljoin('https://fic.pw/', l.url)
			msg = '\n'.join([
					f'request for <{l.query}> => `{l.urlId}` ({l.infoRequestMs}ms)',
					f'```{l.ficInfo}``` ({l.epubCreationMs}ms)',
					f'<{url}> (`{l.hash}`)',
				])
			await botspam_priv.send(msg)

			if not l.isAutomated:
				await sendFicInfo(request_feed, l)

	print('watch_requests: ending')

if __name__ == "__main__":
	client.loop.create_task(watch_requests())
	import secret
	client.run(secret.BOT_TOKEN)

