#!/usr/bin/python
import argparse
import json
import sys
import termcolor
from string import Formatter
from thrift_tools.thrift_message import ThriftMessage



class ColorFormatter(Formatter):
    def __init__(self, msg_dict, nocolor):
        Formatter.__init__(self)
        self.msg_dict = msg_dict
        self.nocolor = nocolor


    def get_value(self, key, args, kwargs):
        """
        :param key: is index (of args) or string (of kwargs)
        :param args: positional arguments tuple, like in .format('one', 'two')
        :param kwargs: keyword arguments dict, like in .format(first='Hodor', last='Hodor!')
           or data={'first':'Hodor','last':'Hodor!'} ; .format(**data)
        :return: the getattr(key) / getitem(key) of args/kwargs/the enclosed property dict
        """
        colored = ''
        if isinstance(key, str):
            if key=='':
                return Formatter.get_value(self,key, args, kwargs)
            try:
                colored = kwargs[key]
            except KeyError:
                colored = self.msg_dict[key]

        elif isinstance(key,int) or isinstance(key,long):
            colored = args[key]

        return colored


    def format_field(self, value, format_spec):
        """
        Meaningful override wonly when format exists (append :...)
        :param value: The (final) stuff before :...
        :param format_spec: The stuff after :
        :return: Colored (or not) value according to format_spec
        """
        if self.nocolor:
            return format(value, format_spec)
        else:
            if format_spec:
                value = self.colorize(str(value), format_spec)
                return value
            else:
                return format(value, format_spec)


    def colorize(self, value, rule):
        """
        :param value: must be str
        :param rule: a single character, one of {'b','g', 'y', 'm', 'r', 'w', 'e', 'c', B'}
        :return: a termcolor.colorize() transformation upon value, according to the rule
        """
        if rule=='b':
            return termcolor.colored(value, 'blue')
        elif rule=='g':
            return termcolor.colored(value, 'green')
        elif rule=='y':
            return termcolor.colored(value, 'yellow')
        elif rule=='m':
            return termcolor.colored(value, 'magenta')
        elif rule=='r':
            return termcolor.colored(value, 'red')
        elif rule=='w':
            return termcolor.colored(value, 'white')
        elif rule=='c':
            return termcolor.colored(value, 'cyan')
        elif rule=='D':
            return termcolor.colored(value, None, attrs=['dark'])
        elif rule=='B':
            return termcolor.colored(value, None, attrs=['bold'])


    def value_color(self, type):
        """Return the color code that will feed colorize() above, according to the type"""
        if self.nocolor:
            return ''

        if type in ('byte', 'i16', 'i32', 'i64'):
            return ':y'
        elif type == 'bool':
            return ':c'
        elif type == 'double':
            return ':m'
        elif type == 'string':
            return ':g'
        elif type=='struct':
            return ''








if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Deserialize Thrift payloads. Only TBinary currently supported")
                                    # epilog="See README for more on the options provided here")
    parser.add_argument('infile', help="read thrift message from binary file (omit for stdin)",
                        nargs='?', type=argparse.FileType('r'), default=sys.stdin)
    parser.add_argument("-d", "--debug", help="more verbose output", action="store_true")
    parser.add_argument("-c", "--nocolor", help="turnoff color output", action="store_true")
    parser.add_argument("--json", help="json output, no colors", action="store_true")
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

    # now we have the parsed Thrift message in 'msg'
    """
    msg.as_dict = {
        "args": {
            "fields": [
                {
                    "field_type": "i32",
                    "field_id": 1,
                    "value": 1
                },
                {
                    "field_type": "struct",
                    "field_id": 2,
                    "value": {
                        "fields": [
                            {
                                "field_type": "i32",
                                "field_id": 1,
                                "value": 15
                            },
                            {
                                "field_type": "i32",
                                "field_id": 2,
                                "value": 10
                            },
                            {
                                "field_type": "i32",
                                "field_id": 3,
                                "value": 2
                            }
                        ]
                    }
                }
            ]
        },
        "header": null,
        "length": 54,
        "seqid": 0,
        "type": "call",
        "method": "calculate"
    }
    """

    # TODO test with Binary, Containers, Double, separate String, i64, byte, bool
    # TODO isolate in function the Exception heuristic



    if args.json:
        print json.dumps(msg.as_dict,indent=2)
    else:
        colorFormatter = ColorFormatter(msg.as_dict, args.nocolor)
        print colorFormatter.format('{type:B} "{method:w}" ({length} bytes) hdr:{header} seqid:{seqid}\nargs:')

        for idx in range(len(msg.as_dict['args']['fields'])):
            f = msg.as_dict['args']['fields'][idx]
            vc = colorFormatter.value_color(f['field_type'])

            field_fmt_str = "args[fields]["+str(idx)+"]"
            format_str = "{"+field_fmt_str+"[field_id]:b} "+ \
                        "<{"+field_fmt_str+"[field_type]}> "+ \
                    (    "{"+field_fmt_str+"[value]"+vc+"}"   if f['field_type'] != 'struct' else '' )
            print colorFormatter.format(format_str)
