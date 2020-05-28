#!/usr/bin/env python3.8

import argparse
import asyncio
import json
import random
import socket
import sys
import time

import paho.mqtt.client as mqtt

#MQTT_BROKER = "localhost"
MQTT_BROKER = "dns-of-NLB"
MQTT_PORT = 1883


t0 = time.time()

def log(*args, **kwargs):
    dt = time.time() - t0
    print(f"+{dt:8.3f}", *args, file=sys.stderr, **kwargs)


class AsyncioHelper:
    def __init__(self, loop, mqtt_client):
        self._loop = loop
        self._mqtt_client = mqtt_client
        self._mqtt_client.on_socket_open = self._on_socket_open
        self._mqtt_client.on_socket_close = self._on_socket_close
        self._mqtt_client.on_socket_register_write = self._on_socket_register_write
        self._mqtt_client.on_socket_unregister_write = self._on_socket_unregister_write

    def _log(self, *args, **kwargs):
        #log("aio_helper:", *args, **kwargs)
        return

    def _on_socket_open(self, _mqtt_client, userdata, sock):
        self._log("on_socket_open()")

        def cb():
            self._log("  -> on_socket_open(): socket is readable, calling loop_read()")
            _mqtt_client.loop_read()

        self._loop.add_reader(sock, cb)
        self.misc = self._loop.create_task(self._misc_loop())

    def _on_socket_close(self, _mqtt_client, userdata, sock):
        self._log("on_socket_close()")
        self._loop.remove_reader(sock)
        self.misc.cancel()

    def _on_socket_register_write(self, _mqtt_client, userdata, sock):
        self._log("on_socet_register_write()")

        def cb():
            self._log("  -> on_socet_register_write(): socket is available, calling loop_write()")
            _mqtt_client.loop_write()

        self._loop.add_writer(sock, cb)

    def _on_socket_unregister_write(self, _mqtt_client, userdata, sock):
        self._log("on_socet_unregister_write()")
        self._loop.remove_writer(sock)

    async def _misc_loop(self):
        self._log("misc_loop() started")
        while self._mqtt_client.loop_misc() == mqtt.MQTT_ERR_SUCCESS:
            try:
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                break

        self._log("misc_loop() finished")


class MuckeMqttBase:

    def __init__(self, mqtt_client_id):
        self._loop = asyncio.get_event_loop()
        self._mqtt_client_id = mqtt_client_id
        self._subscribe_topic = self._mqtt_client_id

        self._mqtt_client = mqtt.Client(client_id=self._mqtt_client_id)
        self._mqtt_client.enable_logger()
        self._mqtt_client.on_connect = self._on_connect
        self._mqtt_client.on_publish = self._on_publish
        self._mqtt_client.on_message = self._on_message
        self._mqtt_client.on_disconnect = self._on_disconnect
        self._asyncio_helper = AsyncioHelper(self._loop, self._mqtt_client)

        self._connected_ev = asyncio.Event()
        self._published_events = {}
        self._next_msg_id = 1

    async def connect(self):
        self._connected_ev.clear()
        self._mqtt_client.connect_async(MQTT_BROKER, MQTT_PORT, 60)
        self._mqtt_client.reconnect()
        await self._connected_ev.wait()
        self._mqtt_client.socket().setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2048)
        self._mqtt_client.subscribe(self._subscribe_topic)

    async def disconnect(self):
        self._mqtt_client.disconnect()

    async def send_msg_async(self, to_topic, title):
        #self._log("send_msg_async(): calling send_msg()")
        rc, mid = self.send_msg(to_topic, title)
        #self._log("send_msg_async(): send_msg() done")
        #self._log("send_msg_async(): setting event")
        #self._published_events[mid] = asyncio.Event()
        #self._log("send_msg_async(): awaiting event")
        #await self._published_events[mid].wait()
        #del self._published_events[mid]
        #self._log("send_msg_async(): awaiting event done")

    def send_msg(self, to_topic, title):
        msg = {
            "from": self._mqtt_client_id,
            "msg_id": self._next_msg_id,
            "t_snd": int(time.time() * 1_000_000),
            "title": title,
        }
        self._log(f"{self._mqtt_client_id}: sending mid={msg['msg_id']} to {to_topic}")
        self._next_msg_id += 1
        rc, mid = self._mqtt_client.publish(to_topic, bytes(json.dumps(msg), encoding="ascii"), qos=0)
        return rc, mid

    def _log(self, *args, **kwargs):
        log(f"{self._mqtt_client_id}:", *args, **kwargs)

    def _on_connect(self, _mqtt_client, userdata, flags, rc):
        self._log(f"connected")
        assert _mqtt_client == self._mqtt_client
        self._connected_ev.set()

    def _on_publish(self, _mqtt_client, userdata, mid):
        self._log(f"published", mid)
        assert _mqtt_client == self._mqtt_client
        if mid in self._published_events:
            self._published_events[mid].set()
        else:
            #self._log("_on_publish(): no event found")
            pass

    def _on_message(self, _mqtt_client, userdata, msg):
        self._log(f"got message")
        assert _mqtt_client == self._mqtt_client
        msg = json.loads(str(msg.payload, "ascii"))
        self._handle_message(msg)

    def _handle_message(self, msg):
        pass

    def _on_disconnect(self, _mqtt_client, userdata, rc):
        self._log(f"disconnected with rc={rc}")
        assert _mqtt_client == self._mqtt_client
        self._connected_ev.clear()


class MuckeWorker(MuckeMqttBase):
    def _handle_message(self, msg):
        now = int(time.time() * 1_000_000)

        # parse message
        msg_from = msg["from"]
        msg_id = msg["msg_id"]
        msg_title = msg["title"]

        # log message and rtt
        self._log(f"-> from {msg_from}, id {msg_id}, title [{msg_title}]")

        # respond to pings
        if msg_title == "ping":
            self.send_msg(msg_from, "pong")

        # log rtt
        if msg_title == "pong":
            msg_t_snd = msg["t_snd"]
            rtt_ms = (now - msg_t_snd) / 1000
            self._log(f"-> rtt {rtt_ms:.0f} ms")


async def do_worker(args, worker_num):
    worker_id = f"{args.prefix}/{worker_num}"
    log(f"{worker_id}: started")
    worker = MuckeWorker(worker_id)
    await worker.connect()
    if args.pub_count > 0:
        t0 = time.time()
        while time.time() - t0 < args.duration:
            d0 = args.delay_min
            d1 = args.delay_max
            await asyncio.sleep(
                (d0 + random.random() * (d1 - d0)) / 1000
            )
            rnd = random.randint(args.pub_n0, args.pub_n0 + args.pub_count - 1)
            pub_topic = f"{args.pub_prefix}/{rnd}"
            await worker.send_msg_async(pub_topic, "ping")
    while True:
        await asyncio.sleep(1)
    worker.disconnect()
    log(f"{worker_id}: finished")


async def main(args):
    tasks = [
        asyncio.ensure_future(do_worker(args, i))
        for i in range(args.n0, args.n0 + args.count)
    ]
    await asyncio.gather(*tasks)



if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Mucke Mannen MQTT Madness")
    parser.add_argument("--prefix",     required=True, type=str, help="worker name prefix")
    parser.add_argument("--n0",         required=True, type=int, help="worker id's starting index")
    parser.add_argument("--count",      required=True, type=int, help="number of workers to spin up")
    parser.add_argument("--duration",   required=True, type=int, help="number of seconds to publish during")
    parser.add_argument("--pub-prefix", required=True, type=str, help="publish topic prefix")
    parser.add_argument("--pub-n0",     required=True, type=int, help="publish topic id's starting index")
    parser.add_argument("--pub-count",  required=True, type=int, help="number of publish-topics")
    parser.add_argument("--delay-min",  required=True, type=int, help="min delay (in ms) between message publishes")
    parser.add_argument("--delay-max",  required=True, type=int, help="max delay (in ms) between message publishes")
    args = parser.parse_args()

    def fatal(*args, **kwargs):
        log(*args, **kwargs)
        sys.exit(-1)

    if args.count < 1:
        fatal("ERROR: cannot have n < 1")

    args.prefix = "mucke-" + args.prefix
    args.pub_prefix = "mucke-" + args.pub_prefix

    asyncio.run(main(args))
