#!/usr/bin/python
import argparse
import json
import sys
from io import StringIO

from thrift_tools.thrift_message import ThriftMessage

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Deserialize Thrift payloads. Only TBinary currently supported")
                                    # epilog="See README for more on the options provided here")
    #parser.add_argument("-i", "--in", help="read binary Thrift message from file", required=False)
    parser.add_argument('infile', help="read thrift message from binary file (omit for stdin)",
                        nargs='?', type=argparse.FileType('r'), default=sys.stdin)
    parser.add_argument("-d", "--debug", help="more verbose output", action="store_true")
    parser.add_argument("-c", "--nocolor", help="turnoff color outout", action="store_true")
    args = parser.parse_args()

    data = []
    if args.infile==sys.stdin:
        if args.debug:
            print "Reading from stdin"

    data = args.infile.read()

    # a bit of brute force to find the start of a message.
    for idx in range(len(data)):
        try:
            data_slice = data[idx:]
            msg, msglen = ThriftMessage.read( data_slice, read_values=True )
        except Exception as ex:
            continue

        if args.debug:
            print "Found Thrift Message at index:%d" % idx

        break

    print json.dumps(msg.as_dict,indent=2)
