# Very Awesome IO Backend


## Local setup

### Create database

You need to be an unhappy Docker bearer in order to run this shitty code. You'll manage to google how to get it.

Then run

```bash
docker-compose up
```
and pray it doesn't fail lol. But if it does, try stopping all other containers using ids from docker ps:
```bash
docker ps
docker stop CONTAINER_ID
```

### Prep virtualenv
Using libraries in Python is nice, easy and doesn't clutter your system partition as long as you use virtualenvs :)
```bash
python3 -m venv venv
pip install -r requirements.txt
```
Probably not all of these requirements are actually needed but I'm trying to minimize the risk of something failing randomly...

### Fill database with some data
We have a script for it!
```bash
python3 data_loading/load.py
```

### Run the server
```bash
python3 server.py
```
