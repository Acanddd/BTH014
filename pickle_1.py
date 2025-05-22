import sys
import io
import codecs
from struct import pack, unpack

dispatch_table = {}

HIGHEST_PROTOCOL = 5
DEFAULT_PROTOCOL = 4

class PickleError(Exception): pass
class PicklingError(PickleError): pass
class UnpicklingError(PickleError): pass

# Pickle opcodes
MARK           = b'('
STOP           = b'.'
POP            = b'0'
POP_MARK       = b'1'
DUP            = b'2'
FLOAT          = b'F'
INT            = b'I'
BININT         = b'J'
BININT1        = b'K'
LONG           = b'L'
BININT2        = b'M'
NONE           = b'N'
PERSID         = b'P'
BINPERSID      = b'Q'
REDUCE         = b'R'
STRING         = b'S'
BINSTRING      = b'T'
SHORT_BINSTRING= b'U'
UNICODE        = b'V'
BINUNICODE     = b'X'
APPEND         = b'a'
BUILD          = b'b'
GLOBAL         = b'c'
DICT           = b'd'
EMPTY_DICT     = b'}'
APPENDS        = b'e'
GET            = b'g'
BINGET         = b'h'
INST           = b'i'
LONG_BINGET    = b'j'
LIST           = b'l'
EMPTY_LIST     = b']'
OBJ            = b'o'
PUT            = b'p'
BINPUT         = b'q'
LONG_BINPUT    = b'r'
SETITEM        = b's'
TUPLE          = b't'
EMPTY_TUPLE    = b')'
SETITEMS       = b'u'
BINFLOAT       = b'G'
TRUE           = b'I01\n'
FALSE          = b'I00\n'
PROTO          = b'\x80'
NEWOBJ         = b'\x81'
EXT1           = b'\x82'
EXT2           = b'\x83'
EXT4           = b'\x84'
TUPLE1         = b'\x85'
TUPLE2         = b'\x86'
TUPLE3         = b'\x87'
NEWTRUE        = b'\x88'
NEWFALSE       = b'\x89'
LONG1          = b'\x8a'
LONG4          = b'\x8b'
BINBYTES       = b'B'
SHORT_BINBYTES = b'C'
SHORT_BINUNICODE = b'\x8c'
BINUNICODE8      = b'\x8d'
BINBYTES8        = b'\x8e'
EMPTY_SET        = b'\x8f'
ADDITEMS         = b'\x90'
FROZENSET        = b'\x91'
NEWOBJ_EX        = b'\x92'
STACK_GLOBAL     = b'\x93'
MEMOIZE          = b'\x94'
FRAME            = b'\x95'
BYTEARRAY8       = b'\x96'
NEXT_BUFFER      = b'\x97'
READONLY_BUFFER  = b'\x98'

_extension_registry = {}
_inverted_registry = {}
_extension_cache = {}

def encode_long(x):
    if x == 0:
        return b''
    nbytes = (x.bit_length() >> 3) + 1
    result = x.to_bytes(nbytes, byteorder='little', signed=True)
    if x < 0 and nbytes > 1:
        if result[-1] == 0xff and (result[-2] & 0x80) != 0:
            result = result[:-1]
    return result

def decode_long(data):
    return int.from_bytes(data, byteorder='little', signed=True)

class _Framer:
    _FRAME_SIZE_MIN = 4
    _FRAME_SIZE_TARGET = 64 * 1024
    def __init__(self, file_write):
        self.file_write = file_write
        self.current_frame = None
    def start_framing(self):
        self.current_frame = io.BytesIO()
    def end_framing(self):
        if self.current_frame and self.current_frame.tell() > 0:
            self.commit_frame(force=True)
            self.current_frame = None
    def commit_frame(self, force=False):
        if self.current_frame:
            f = self.current_frame
            if f.tell() >= self._FRAME_SIZE_TARGET or force:
                data = f.getbuffer()
                write = self.file_write
                if len(data) >= self._FRAME_SIZE_MIN:
                    write(FRAME + pack("<Q", len(data)))
                write(data)
                self.current_frame = io.BytesIO()
    def write(self, data):
        if self.current_frame:
            return self.current_frame.write(data)
        else:
            return self.file_write(data)
    def write_large_bytes(self, header, payload):
        write = self.file_write
        if self.current_frame:
            self.commit_frame(force=True)
        write(header)
        write(payload)

class _Pickler:
    def __init__(self, file, protocol=None, *, fix_imports=True, buffer_callback=None):
        if protocol is None:
            protocol = DEFAULT_PROTOCOL
        if protocol < 0:
            protocol = HIGHEST_PROTOCOL
        elif not 0 <= protocol <= HIGHEST_PROTOCOL:
            raise ValueError("pickle protocol must be <= %d" % HIGHEST_PROTOCOL)
        if buffer_callback is not None and protocol < 5:
            raise ValueError("buffer_callback needs protocol >= 5")
        self._buffer_callback = buffer_callback
        try:
            self._file_write = file.write
        except AttributeError:
            raise TypeError("file must have a 'write' attribute")
        self.framer = _Framer(self._file_write)
        self.write = self.framer.write
        self._write_large_bytes = self.framer.write_large_bytes
        self.memo = {}
        self.proto = int(protocol)
        self.bin = protocol >= 1
        self.fast = 0
        self.fix_imports = fix_imports and protocol < 3
    def clear_memo(self):
        self.memo.clear()
    def dump(self, obj):
        if not hasattr(self, "_file_write"):
            raise PicklingError("Pickler.__init__() was not called by %s.__init__()" % (self.__class__.__name__,))
        if self.proto >= 2:
            self.write(PROTO + pack("<B", self.proto))
        if self.proto >= 4:
            self.framer.start_framing()
        self.save(obj)
        self.write(STOP)
        self.framer.end_framing()
    def memoize(self, obj):
        if self.fast:
            return
        assert id(obj) not in self.memo
        idx = len(self.memo)
        self.write(self.put(idx))
        self.memo[id(obj)] = idx, obj
    def put(self, idx):
        if self.proto >= 4:
            return MEMOIZE
        elif self.bin:
            if idx < 256:
                return BINPUT + pack("<B", idx)
            else:
                return LONG_BINPUT + pack("<I", idx)
        else:
            return PUT + repr(idx).encode("ascii") + b'\n'
    def get(self, i):
        if self.bin:
            if i < 256:
                return BINGET + pack("<B", i)
            else:
                return LONG_BINGET + pack("<I", i)
        return GET + repr(i).encode("ascii") + b'\n'
    def save(self, obj, save_persistent_id=True):
        self.framer.commit_frame()
        x = self.memo.get(id(obj))
        if x is not None:
            self.write(self.get(x[0]))
            return
        t = type(obj)
        if t is type(None):
            self.write(NONE)
            self.memoize(obj)
        elif t is bool:
            if self.proto >= 2:
                self.write(NEWTRUE if obj else NEWFALSE)
            else:
                self.write(TRUE if obj else FALSE)
            self.memoize(obj)
        elif t is int:
            if self.bin:
                if obj >= 0:
                    if obj <= 0xff:
                        self.write(BININT1 + pack("<B", obj))
                        self.memoize(obj)
                        return
                    if obj <= 0xffff:
                        self.write(BININT2 + pack("<H", obj))
                        self.memoize(obj)
                        return
                if -0x80000000 <= obj <= 0x7fffffff:
                    self.write(BININT + pack("<i", obj))
                    self.memoize(obj)
                    return
            if self.proto >= 2:
                encoded = encode_long(obj)
                n = len(encoded)
                if n < 256:
                    self.write(LONG1 + pack("<B", n) + encoded)
                else:
                    self.write(LONG4 + pack("<i", n) + encoded)
                self.memoize(obj)
                return
            if -0x80000000 <= obj <= 0x7fffffff:
                self.write(INT + repr(obj).encode("ascii") + b'\n')
            else:
                self.write(LONG + repr(obj).encode("ascii") + b'L\n')
            self.memoize(obj)
        elif t is float:
            if self.bin:
                self.write(BINFLOAT + pack('>d', obj))
            else:
                self.write(FLOAT + repr(obj).encode("ascii") + b'\n')
            self.memoize(obj)
        elif t is str:
            if self.bin:
                encoded = obj.encode('utf-8', 'surrogatepass')
                n = len(encoded)
                if n <= 0xff and self.proto >= 4:
                    self.write(SHORT_BINUNICODE + pack("<B", n) + encoded)
                elif n > 0xffffffff and self.proto >= 4:
                    self._write_large_bytes(BINUNICODE8 + pack("<Q", n), encoded)
                elif n >= self.framer._FRAME_SIZE_TARGET:
                    self._write_large_bytes(BINUNICODE + pack("<I", n), encoded)
                else:
                    self.write(BINUNICODE + pack("<I", n) + encoded)
            else:
                tmp = obj.replace("\\", "\\u005c").replace("\0", "\\u0000").replace("\n", "\\u000a").replace("\r", "\\u000d").replace("\x1a", "\\u001a")
                self.write(UNICODE + tmp.encode('raw-unicode-escape') + b'\n')
            self.memoize(obj)
        elif t is bytes:
            n = len(obj)
            if self.proto < 3:
                if not obj:
                    self.save_reduce(bytes, (), obj=obj)
                else:
                    self.save_reduce(codecs.encode, (str(obj, 'latin1'), 'latin1'), obj=obj)
                return
            if n <= 0xff:
                self.write(SHORT_BINBYTES + pack("<B", n) + obj)
            elif n > 0xffffffff and self.proto >= 4:
                self._write_large_bytes(BINBYTES8 + pack("<Q", n), obj)
            elif n >= self.framer._FRAME_SIZE_TARGET:
                self._write_large_bytes(BINBYTES + pack("<I", n), obj)
            else:
                self.write(BINBYTES + pack("<I", n) + obj)
            self.memoize(obj)
        elif t is list:
            if self.bin:
                self.write(EMPTY_LIST)
            else:
                self.write(MARK + LIST)
            self.memoize(obj)
            for item in obj:
                self.save(item)
                self.write(APPEND)
        elif t is tuple:
            if not obj:
                if self.bin:
                    self.write(EMPTY_TUPLE)
                else:
                    self.write(MARK + TUPLE)
                return
            n = len(obj)
            if n <= 3 and self.proto >= 2:
                for element in obj:
                    self.save(element)
                self.write([EMPTY_TUPLE, TUPLE1, TUPLE2, TUPLE3][n])
                self.memoize(obj)
                return
            self.write(MARK)
            for element in obj:
                self.save(element)
            self.write(TUPLE)
            self.memoize(obj)
        elif t is dict:
            if self.bin:
                self.write(EMPTY_DICT)
            else:
                self.write(MARK + DICT)
            self.memoize(obj)
            for k, v in obj.items():
                self.save(k)
                self.save(v)
                self.write(SETITEM)
        else:
            raise PicklingError(f"Unsupported type: {t}")

    def save_reduce(self, func, args, state=None, listitems=None, dictitems=None, state_setter=None, *, obj=None):
        self.save(func)
        self.save(args)
        self.write(REDUCE)
        if obj is not None:
            self.memoize(obj)
        if listitems is not None:
            for item in listitems:
                self.save(item)
                self.write(APPEND)
        if dictitems is not None:
            for k, v in dictitems:
                self.save(k)
                self.save(v)
                self.write(SETITEM)
        if state is not None:
            self.save(state)
            self.write(BUILD)

def _dump(obj, file, protocol=None, *, fix_imports=True, buffer_callback=None):
    _Pickler(file, protocol, fix_imports=fix_imports, buffer_callback=buffer_callback).dump(obj)

def _dumps(obj, protocol=None, *, fix_imports=True, buffer_callback=None):
    f = io.BytesIO()
    _Pickler(f, protocol, fix_imports=fix_imports, buffer_callback=buffer_callback).dump(obj)
    return f.getvalue()

dump = _dump
dumps = _dumps
