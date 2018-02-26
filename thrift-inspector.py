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





class ThriftJsonEncoder(json.JSONEncoder):
    def default(self, o):
        """Implement this method in a subclass such that it returns
        a serializable object for ``o``, or calls the base implementation
        (to raise a ``TypeError``).

        For example, to support arbitrary iterators, you could
        implement default like this::

            def default(self, o):
                try:
                    iterable = iter(o)
                except TypeError:
                    pass
                else:
                    return list(iterable)
                # Let the base class default method raise the TypeError
                return JSONEncoder.default(self, o)

        """
        return iter(o.as_dict())






def recursive_print(cur_list, cur_field_literal, cur_indent):
    for idx in range(len(cur_list)):
        f = cur_list[idx]
        vc = colorFormatter.value_color(f['field_type'])
        is_struct = f['field_type'] == 'struct'

        field_fmt_str = cur_field_literal + str(idx) + "]"
        format_str = cur_indent * ' ' +  "{" + field_fmt_str + "[field_id]:b} " + \
                     cur_indent * ' ' + "<{" + field_fmt_str + "[field_type]}> " + \
                    (cur_indent * ' ' +  "{" + field_fmt_str + "[value]" + vc + "}" if not is_struct else '')
        print colorFormatter.format(format_str)

        if is_struct:
            cur_list = cur_list[idx]['value']['fields']
            cur_field_literal = cur_field_literal+str(idx)+"][value][fields]["
            cur_indent += 2
            recursive_print(
                cur_list,
                cur_field_literal,
                cur_indent
            )




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
    for offset in range(len(data)):
        try:
            data_slice = data[offset:]
            msg, msglen = ThriftMessage.read( data_slice, read_values=True )
        except Exception as ex:
            continue

        if args.debug:
            print "Found Thrift Message at offest:%d" % offset

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
        print json.dumps(msg.as_dict, indent=2, cls=ThriftJsonEncoder)
        """
        TODO fixme
        When collections like sets are found, JSONB encoder can't parse them (not supported)
        
        Override json/encoder/default() to support set() or ThriftStruct
        """

    else:
        colorFormatter = ColorFormatter(msg.as_dict, args.nocolor)
        print colorFormatter.format('{type:B} "{method:w}" ({length} bytes) hdr:{header} seqid:{seqid}\nargs:')

        cur_list = msg.as_dict['args']['fields']
        cur_field_literal = "args[fields]["
        cur_indent = 0
        recursive_print(
            cur_list,
            cur_field_literal,
            cur_indent
        )
