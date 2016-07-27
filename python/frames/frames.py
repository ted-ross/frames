##
## Licensed to the Apache Software Foundation (ASF) under one
## or more contributor license agreements.  See the NOTICE file
## distributed with this work for additional information
## regarding copyright ownership.  The ASF licenses this file
## to you under the Apache License, Version 2.0 (the
## "License"); you may not use this file except in compliance
## with the License.  You may obtain a copy of the License at
##
##   http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing,
## software distributed under the License is distributed on an
## "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
## KIND, either express or implied.  See the License for the
## specific language governing permissions and limitations
## under the License.
##

import socket

FRAME_INVALID     = 0x00
FRAME_OPEN        = 0x10
FRAME_BEGIN       = 0x11
FRAME_ATTACH      = 0x12
FRAME_FLOW        = 0x13
FRAME_TRANSFER    = 0x14
FRAME_DISPOSITION = 0x15
FRAME_DETACH      = 0x16
FRAME_END         = 0x17
FRAME_CLOSE       = 0x18

def performative(code):
    return b"\x00\x53" + chr(code)

def uint8(val):
    return chr(val & 0x000000ff)

def uint32(val):
    octets  = chr((val & 0xff000000) >> 24)
    octets += chr((val & 0x00ff0000) >> 16)
    octets += chr((val & 0x0000ff00) >> 8)
    octets += chr(val & 0x000000ff)
    return octets

def encodeNull():
    return b"\x40"

def encodeInt(val):
    if val <= 255:
        return b"\x52" + uint8(val)
    else:
        return b"\x70" + uint32(val)

def encodeBool(val):
    if val:
        return b"\x41"
    return b"\x42"

def encodeString(s):
    if len(s) <= 255:
        octets = b"\xa1" + uint8(len(s))
    else:
        octets = b"\xb1" + uint32(len(s))
    return octets + s


class AmqpList(object):
    def __init__(self):
        self.count = 0
        self.body  = b""

    def insert(self, field):
        self.count += 1
        self.body  += field

    @property
    def octets(self):
        return b"\xd0" + uint32(len(self.body) + 4) + uint32(self.count) + self.body


class Transport(object):
    """
    """
    def __init__(self, host, port):
        self.host = host
        self.port = port

        self.sock = socket.socket()
        self.sock.connect((host, port))

        self.sock.send(b"AMQP\x00\x01\x00\x00")
        version = self.sock.recv(8)
        if version != b"AMQP\x00\x01\x00\x00":
            raise Exception("Invalid Protocol Version %r" % version)

    def get_frame(self):
        header = self.sock.recv(8)
        length = ord(header[3]) + (ord(header[2]) << 8) + (ord(header[1]) << 16) + (ord(header[0]) << 24)
        ftype  = ord(header[5])

        if ftype == 0:
            frame_body = self.sock.recv(length)
            return AmqpFrame(frame_body)
        return None

    def send_frame(self, octets):
        print "send_frame: %r" % octets
        length = len(octets) + 8
        header = uint32(length)
        header += "\x02\x00\x00\x00"
        self.sock.send(header + octets)

    def open(self, container_id, **kwargs):
        args = AmqpList()
        args.insert(encodeString(container_id))
        self.send_frame(performative(FRAME_OPEN) + args.octets)

    def begin(self, in_window, out_window, remote_chan=None, **kwargs):
        args = AmqpList()
        args.insert(encodeNull())    # remote-channel
        args.insert(encodeInt(0))    # next-outgoing-id
        args.insert(encodeInt(in_window))  # incoming-window
        args.insert(encodeInt(out_window)) # outgoing-window
        self.send_frame(performative(FRAME_BEGIN) + args.octets)

    def attach(self, name, handle, role, **kwargs):
        args = AmqpList()
        args.insert(encodeString(name))  # name
        args.insert(encodeInt(handle))   # handle
        args.insert(encodeBool(role))    # role
        self.send_frame(performative(FRAME_ATTACH) + args.octets)

    def flow(self, next_in_id, in_window, next_out_id, out_window, **kwargs):
        args = AmqpList()
        args.insert(encodeInt(next_in_id))
        args.insert(encodeInt(in_window))
        args.insert(encodeInt(next_out_id))
        args.insert(encodeInt(out_window))
        if "handle" in kwargs:
            args.insert(encodeInt(kwargs["handle"]))
        self.send_frame(performative(FRAME_FLOW) + args.octets)

    def transfer(self, handle, **kwargs):
        pass

    def disposition(self, role, first, **kwargs):
        pass

    def detach(self, handle, **kwargs):
        pass

    def end(self, **kwargs):
        pass

    def close(self, **kwargs):
        pass

class AmqpFrame(object):
    """
    """
    def __init__(self, octets):
        pass

