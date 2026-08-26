#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json

from six import BytesIO, indexbytes, int2byte
from twisted.internet import endpoints, reactor
from twisted.internet.defer import Deferred, inlineCallbacks
from twisted.internet.protocol import ClientFactory, Factory, Protocol
from twisted.trial import unittest
from twisted.web.client import Agent, FileBodyProducer, readBody
from twisted.web.http_headers import Headers
from twisted.web.resource import Resource
from twisted.web.server import Site

from txsockjs.factory import SockJSResource
from txsockjs.websockets import _makeFrame


class RecordingProtocol(Protocol):
    def dataReceived(self, data):
        self.factory.messages.append(data)
        if not self.factory.message_received.called:
            self.factory.message_received.callback(data)
        if data == b"sequence":
            self.transport.writeSequence([b"hello"])
        else:
            self.transport.write(b"echo:" + data)


class RecordingFactory(Factory):
    protocol = RecordingProtocol

    def __init__(self):
        self.message_received = Deferred()
        self.messages = []


def masked_frame(payload):
    mask = b"\x01\x02\x03\x04"
    masked = b"".join(
        int2byte(indexbytes(payload, index) ^ indexbytes(mask, index % 4))
        for index in range(len(payload))
    )
    return b"\x81" + int2byte(0x80 | len(payload)) + mask + masked


class WebSocketClient(Protocol):
    def connectionMade(self):
        self.received = b""
        self.transport.write(
            b"GET /ws/000/session/websocket HTTP/1.1\r\n"
            b"Host: 127.0.0.1\r\n"
            b"Upgrade: websocket\r\n"
            b"Connection: Upgrade\r\n"
            b"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
            b"Sec-WebSocket-Version: 13\r\n\r\n"
        )

    def dataReceived(self, data):
        self.received += data
        if b"\r\n\r\n" in self.received and not self.factory.frame_sent:
            self.factory.frame_sent = True
            self.transport.write(masked_frame(b'["hello"]'))
        if self.factory.expected_frame in self.received:
            if not self.factory.finished.called:
                self.factory.finished.callback(self.received)
            self.transport.loseConnection()

    def connectionLost(self, reason):
        if not self.factory.finished.called:
            self.factory.finished.errback(reason)


class WebSocketClientFactory(ClientFactory):
    protocol = WebSocketClient

    def __init__(self):
        self.expected_frame = _makeFrame(b'a["echo:hello"]')
        self.finished = Deferred()
        self.frame_sent = False


class HixieClient(Protocol):
    def connectionMade(self):
        self.received = b""
        self.transport.write(
            b"GET /ws/000/hixie-session/websocket HTTP/1.1\r\n"
            b"Host: 127.0.0.1\r\n"
            b"Upgrade: WebSocket\r\n"
            b"Connection: Upgrade\r\n"
            b"Origin: http://example.com\r\n\r\n"
        )

    def dataReceived(self, data):
        self.received += data
        if b"\r\n\r\n" in self.received and not self.factory.frame_sent:
            self.factory.frame_sent = True
            self.transport.write(b'\x00["hello"]\xff')
        if self.factory.expected_frame in self.received:
            if not self.factory.finished.called:
                self.factory.finished.callback(self.received)
            self.transport.loseConnection()

    def connectionLost(self, reason):
        if not self.factory.finished.called:
            self.factory.finished.errback(reason)


class HixieClientFactory(ClientFactory):
    protocol = HixieClient

    def __init__(self):
        self.expected_frame = b'\x00a["echo:hello"]\xff'
        self.finished = Deferred()
        self.frame_sent = False


class SockJSNetworkTests(unittest.TestCase):
    timeout = 10

    def setUp(self):
        self.clients = []
        self.wrapped_factory = RecordingFactory()
        self.sockjs = SockJSResource(
            self.wrapped_factory,
            options={"heartbeat": 0.05, "timeout": 1},
        )
        root = Resource()
        root.putChild(b"ws", self.sockjs)
        self.listener = reactor.listenTCP(0, Site(root), interface="127.0.0.1")
        self.port = self.listener.getHost().port
        self.base_url = ("http://127.0.0.1:%d/ws" % self.port).encode("ascii")
        self.agent = Agent(reactor)

    @inlineCallbacks
    def tearDown(self):
        for client in self.clients:
            if getattr(client, "transport", None) is not None:
                client.transport.loseConnection()
        for session in list(self.sockjs._sessions.values()):
            session.disconnect()
        if self.listener is not None:
            yield self.listener.stopListening()

    @inlineCallbacks
    def test_info_and_xhr_factory_bytes_flow(self):
        response = yield self.agent.request(b"GET", self.base_url + b"/info")
        body = yield readBody(response)
        info = json.loads(body.decode("ascii"))
        self.assertEqual(response.code, 200)
        self.assertEqual(info["websocket"], True)
        self.assertEqual(info["cookie_needed"], False)
        self.assertEqual(
            response.headers.getRawHeaders(b"access-control-allow-origin"),
            [b"*"],
        )
        self.assertEqual(
            response.headers.getRawHeaders(b"cache-control"),
            [b"no-store, no-cache, must-revalidate, max-age=0"],
        )

        response = yield self.agent.request(
            b"POST", self.base_url + b"/000/session/xhr"
        )
        self.assertEqual((yield readBody(response)), b"o\n")

        response = yield self.agent.request(
            b"POST",
            self.base_url + b"/000/session/xhr_send",
            Headers({b"content-type": [b"application/json"]}),
            FileBodyProducer(BytesIO(b'["hello"]')),
        )
        yield readBody(response)
        self.assertEqual(response.code, 204)
        self.assertEqual((yield self.wrapped_factory.message_received), b"hello")

        response = yield self.agent.request(
            b"POST", self.base_url + b"/000/session/xhr"
        )
        self.assertEqual((yield readBody(response)), b'a["echo:hello"]\n')

    @inlineCallbacks
    def test_xhr_factory_write_sequence_preserves_accepted_framing(self):
        response = yield self.agent.request(
            b"POST", self.base_url + b"/000/sequence-session/xhr"
        )
        self.assertEqual((yield readBody(response)), b"o\n")

        response = yield self.agent.request(
            b"POST",
            self.base_url + b"/000/sequence-session/xhr_send",
            Headers({b"content-type": [b"application/json"]}),
            FileBodyProducer(BytesIO(b'["sequence"]')),
        )
        yield readBody(response)
        self.assertEqual(response.code, 204)

        response = yield self.agent.request(
            b"POST", self.base_url + b"/000/sequence-session/xhr"
        )
        self.assertEqual((yield readBody(response)), b'a["hello"]\n')

    @inlineCallbacks
    def test_jsonp_preserves_accepted_callback_framing(self):
        response = yield self.agent.request(
            b"GET", self.base_url + b"/000/jsonp-session/jsonp?c=callback"
        )
        body = yield readBody(response)

        self.assertEqual(response.code, 200)
        self.assertEqual(body, b'callback("o");\r\n')

    @inlineCallbacks
    def test_streaming_fallbacks_preserve_accepted_framing(self):
        self.sockjs._options["streaming_limit"] = 0

        response = yield self.agent.request(
            b"GET", self.base_url + b"/000/event-session/eventsource"
        )
        body = yield readBody(response)
        self.assertEqual(response.code, 200)
        self.assertEqual(body, b"\r\ndata: o\r\n\r\n")

        response = yield self.agent.request(
            b"GET", self.base_url + b"/000/html-session/htmlfile?c=callback"
        )
        body = yield readBody(response)
        self.assertEqual(response.code, 200)
        self.assertIn(b"var c = parent.callback;", body)
        self.assertIn(b'<script>\np("o");\n</script>\r\n', body)

    @inlineCallbacks
    def test_xhr_heartbeat_uses_default_transport_path(self):
        response = yield self.agent.request(
            b"POST", self.base_url + b"/000/heartbeat-session/xhr"
        )
        self.assertEqual((yield readBody(response)), b"o\n")

        response = yield self.agent.request(
            b"POST", self.base_url + b"/000/heartbeat-session/xhr"
        )
        self.assertEqual((yield readBody(response)), b"h\n")

    @inlineCallbacks
    def test_websocket_handshake_and_protocol_bytes_flow(self):
        client_factory = WebSocketClientFactory()
        endpoint = endpoints.TCP4ClientEndpoint(
            reactor, "127.0.0.1", self.port, timeout=5
        )
        client = yield endpoint.connect(client_factory)
        self.clients.append(client)
        received = yield client_factory.finished

        headers, _, frames = received.partition(b"\r\n\r\n")
        self.assertIn(b"HTTP/1.1 101", headers)
        self.assertIn(
            b"sec-websocket-accept: s3pplmbitxaq9kygzzhzrbk+xoo=",
            headers.lower(),
        )
        self.assertIn(_makeFrame(b"o"), frames)
        self.assertIn(client_factory.expected_frame, frames)
        self.assertEqual(self.wrapped_factory.messages, [b"hello"])

    @inlineCallbacks
    def test_hixie_websocket_fallback_preserves_accepted_bytes_flow(self):
        client_factory = HixieClientFactory()
        endpoint = endpoints.TCP4ClientEndpoint(
            reactor, "127.0.0.1", self.port, timeout=5
        )
        client = yield endpoint.connect(client_factory)
        self.clients.append(client)
        received = yield client_factory.finished

        headers, _, frames = received.partition(b"\r\n\r\n")
        self.assertIn(b"HTTP/1.1 101", headers)
        self.assertIn(b"websocket-origin: http://example.com", headers.lower())
        self.assertIn(b"\x00o\xff", frames)
        self.assertIn(client_factory.expected_frame, frames)
