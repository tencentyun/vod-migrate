# -*- coding: utf-8 -*-
import sys
import hashlib
from six import text_type, PY3

fs_coding = sys.getfilesystemencoding()


def get_file_md5(filename):
    f = open(filename, 'rb')
    md5hash = hashlib.md5()
    content = f.read()
    md5hash.update(content)

    return md5hash.hexdigest()


def to_printable_str(s):
    if PY3:
        return s

    if isinstance(s, text_type):
        return s.encode(fs_coding)
    else:
        return s