#!/usr/bin/env python3
from typing import Set, List
import re
import sys
import json
import traceback
import urllib.parse
import datetime
from bs4 import BeautifulSoup # type: ignore
import discord # type: ignore
import asyncio
from oil import oil # type: ignore

client = discord.Client()

ETYPE_COLORS = {
		'epub': discord.Colour.blue(),
		'html': discord.Colour.gold(),
		'mobi': discord.Colour.green(),
		'pdf': discord.Colour.red(),
	}

API_PREFIX = 'https://fichub.net/api/v0/epub?q='
API_AUTO_PREFIX = 'https://fichub.net/api/v0/epub?automated=true&q='

def lookup(query: str):
	import requests
	try:
		req = requests.get(API_PREFIX + query)
		res = req.json()
		return res
	except:
		return {'error':-1,'msg':'lookup failed :('}

def automatedLookup(query: str):
	import requests
	try:
		req = requests.get(API_AUTO_PREFIX + query)
		res = req.json()
		return res
	except:
		return {'error':-1,'msg':'lookup failed :('}

class RequestSource:
	def __init__(self, id_, created_, isAutomated_, route_, description_):
		self.id = id_
		self.created = created_
		self.isAutomated = isAutomated_
		self.route = route_
		self.description = description_

	@staticmethod
	def select(id_: int) -> 'RequestSource':
		with oil.open() as db, db.cursor() as curs:
			curs.execute('''
				select rs.id, rs.created, rs.isAutomated, rs.route, rs.description
				from requestSource rs
				where rs.id = %s
			''', (id_,))
			r = curs.fetchone()
			return None if r is None else RequestSource(*r)

class RequestLog:
	def __init__(self, id_, created_, sourceId_, etype_, query_, infoRequestMs_,
			urlId_, ficInfo_, exportMs_, exportFileName_, exportFileHash_, url_):
		self.id = id_
		self.created = created_
		self.sourceId = sourceId_
		self.etype = etype_
		self.query = query_
		self.infoRequestMs = infoRequestMs_
		self.urlId = urlId_
		self.ficInfo = ficInfo_
		self.exportMs = exportMs_
		self.exportFileName = exportFileName_
		self.exportFileHash = exportFileHash_
		self.url = url_

	@staticmethod
	def maxId() -> int:
		with oil.open() as db, db.cursor() as curs:
			curs.execute('select max(id) from requestLog')
			r = curs.fetchone()
			return r[0]
		return -1

	@staticmethod
	def fetchAfter(after):
		with oil.open() as db, db.cursor() as curs:
			curs.execute('''
				select r.id, r.created, r.sourceId, r.etype, r.query, r.infoRequestMs,
					r.urlId, r.ficInfo, r.exportMs, r.exportFileName, r.exportFileHash,
					r.url
				from requestLog r
				where id > %s and (r.exportFileHash is null or not exists (
					select 1
					from requestLog r2
					where r2.exportFileHash = r.exportFileHash
						and r2.id < r.id
				))
				''', (after,))
			ls = [RequestLog(*r) for r in curs.fetchall()]
			return ls
		return []

	@staticmethod
	def mostRecentByUrlId(urlId):
		with oil.open() as db, db.cursor() as curs:
			curs.execute('''
			select r.id, r.created, r.sourceId, r.etype, r.query, r.infoRequestMs,
				r.urlId, r.ficInfo, r.exportMs, r.exportFileName, r.exportFileHash,
				r.url
			from requestLog r
			where urlId = %s
			order by created desc limit 1''', (urlId,))
			r = curs.fetchone()
			if r is None:
				return None
			return RequestLog(*r)

@client.event
async def on_ready():
	print('We have logged in as {0.user}'.format(client))

def escape_msg(msg: str) -> str:
	return discord.utils.escape_mentions(discord.utils.escape_markdown(msg))

async def sendFicInfo(channel, l: RequestLog):
	try:
		url = urllib.parse.urljoin('https://fichub.net/', l.url)
		info = json.loads(l.ficInfo)
		descSoup = BeautifulSoup(info['desc'], 'lxml')
		infoTime = f'{l.infoRequestMs/1000.0:.3f}s'
		exportTime = f'{l.exportMs/1000.0:.3f}s'
		msg = f'request for <{info["source"]}> => `{l.urlId}` ({infoTime})'
		msg += f', generated {l.etype} in {exportTime}'
		title = escape_msg(f'{info["title"]} by {info["author"]}')
		# description cannot exceed 2048 bytes
		desc = f'\n{info["words"]} words in {info["chapters"]} chapters'
		desc2 = escape_msg(descSoup.get_text())
		if len(desc2) >= 2040 - len(desc):
			desc = desc2[:2040 - len(desc)] + '...' + desc
		else:
			desc = desc2 + desc
		e = discord.Embed(title=title, description=desc, url=url)
		if l.etype in ETYPE_COLORS:
			e.colour = ETYPE_COLORS[l.etype]
		await channel.send(msg, embed=e)
		return True
	except Exception as e:
		traceback.print_exc()
		print(e)
		print('sendFicInfo: error: ^')
	return False

async def sendDevFicInfo(channel, l: RequestLog):
	url = urllib.parse.urljoin('https://fichub.net/', l.url)
	m1 = f'request for {l.etype} of <{l.query}> => `{l.urlId}` ({l.infoRequestMs}ms)'
	m2 = f'`````` ({l.exportMs}ms)'
	m3 = f'<{url}> (`{l.exportFileHash}`)'
	msg = '\n'.join([m1, m2, m3])

	leftover = 1800 - len(msg) - 16
	lfi = l.ficInfo[0:leftover]
	m2 = f'```{lfi}``` ({l.exportMs}ms)'
	msg = '\n'.join([m1, m2, m3])

	try:
		await channel.send(msg)
	except Exception as e:
		traceback.print_exc()
		print(e)
		print('sendDevFicInfo: error: ^')

async def sendErrorLog(channel, l: RequestLog):
	print(f'failed request {l.id}')
	try:
		msg = f'failed request {l.id}: ```' + str(l.__dict__)
		while len(msg) > 1800:
			await channel.send(msg[:1800] + '```')
			msg = '```' + msg[1800:]
		await channel.send(msg + '```')
	except Exception as e:
		traceback.print_exc()
		print(e)
		print('sendErrorLog: error: unable to report error :( ^')

async def delerr_q(chan, errq) -> int:
	print(f'delerr_q({errq})')
	cnt=0
	async for pm in chan.history(limit=500):
		if pm.author == client.user and pm.content.find(errq) >= 0:
			await pm.delete()
			cnt += 1
	return cnt

async def delerr(msg) -> None:
	prefix = '!delerr '
	if not msg.content.startswith(prefix):
		return
	errq = msg.content[len(prefix):].strip()
	cnt = await delerr_q(msg.channel, errq)
	await msg.channel.send(f'deleted {cnt} matching messages')
	return

async def cleanup_retry(chan, q) -> int:
	print(f'cleanup_retry({q})')
	try:
		lr = automatedLookup(q)
		if 'err' in lr and int(lr['err']) != 0:
			return 0
		if 'error' in lr and int(lr['error']) != 0:
			return 0
	except:
		return 0
	print(f'cleanup_retry({q}): now successful, deleting old errors')
	cnt=await delerr_q(chan, f", 'query': '{q}', ")
	print(f'cleanup_retry({q}): now successful, deleted {cnt} old errors')
	return cnt

async def cleanup(msg) -> None:
	prefix = '!cleanup'
	if not msg.content.startswith(prefix):
		return
	validPrefixes = [
			'www.fanfiction.net/s/',
			'm.fanfiction.net/s/',
			'fanfiction.net/s/',
			'www.fictionpress.com/s/',
			'm.fictionpress.com/s/',
			'archiveofourown.org/works/',
			'forums.sufficientvelocity.com/threads',
			'forums.spacebattles.com/threads',
			'forum.questionablequesting.com/threads',
			'www.royalroad.com/fiction/',
			'royalroad.com/fiction/',
			'harrypotterfanfiction.com/',
			'(?:[^\.]*).adult-fanfiction.org/story.php?'
		]
	toRecheck: Set[str] = set()
	toRecheckList: List[str] = []
	msgCount = 0
	async for pm in msg.channel.history(limit=500):
		if pm.author != client.user:
			continue
		for vp in validPrefixes:
			for proto in ['', 'https://', 'http://']:
				rr = re.match(f".*'epub', 'query': '({proto}{vp}[^']*)', .*", pm.content)
				if rr is None:
					continue
				query = rr.group(1)
				msgCount += 1
				if query in toRecheck:
					continue
				toRecheck |= { query }
				toRecheckList += [ query ]
	print(f'cleanup: going to recheck {len(toRecheck)} queries in {msgCount} messages: {toRecheck})')
	cnt=0
	for i in range(len(toRecheckList)):
		q = toRecheckList[i]
		print(f'cleanup: going to recheck query {i + 1}/{len(toRecheck)}: {q})')
		cnt += await cleanup_retry(msg.channel, q)
		await asyncio.sleep(1)
	await msg.channel.send(f'finished cleanup: removed {cnt} now-successful')

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
	if message.content.startswith('!delerr '):
		await delerr(message)
		return
	if message.content.startswith('!cleanup'):
		await cleanup(message)
		return

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

async def watch_requests():
	await client.wait_until_ready()
	botspam_priv = client.get_channel(754481638695501866) # #botspam-priv
	botspam_err  = client.get_channel(785868128096747540) # #botspam-err
	request_feed = client.get_channel(754779814740754492) # #request-feed

	maxId = RequestLog.maxId()
	if len(sys.argv) > 1 and sys.argv[1].isnumeric():
		maxId = int(sys.argv[1])
		print(f'set maxId to {maxId} from cli')

	await botspam_priv.send('started up')
	while not client.is_closed():
		await asyncio.sleep(3)
		ls = RequestLog.fetchAfter(maxId)
		for l in ls:
			maxId = max(maxId, l.id)

			if l.exportFileHash is None:
				await sendErrorLog(botspam_err, l)
				continue

			#await sendDevFicInfo(botspam_priv, l)

			rs = RequestSource.select(l.sourceId)
			if not rs.isAutomated:
				await sendFicInfo(request_feed, l)

			await asyncio.sleep(1)

	print('watch_requests: ending')

if __name__ == "__main__":
	client.loop.create_task(watch_requests())
	import secret
	client.run(secret.BOT_TOKEN)

