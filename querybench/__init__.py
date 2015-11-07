import ast
import collections
import inspect
import itertools
import msgpack
import sully
import types

TAB = '  '
SELF_ARG = 'SELF__'
CONST_PREFIX = 'CONST__'
PACKED_TYPES = (list, dict)
PIPELINED_CODE = """
local __PIPELINE_RESULTS = {}

local __PIPE_ADD = function(key, value)
  if __PIPELINE_RESULTS[key] == nil then
    __PIPELINE_RESULTS[key] = {}
  end

  table.insert(__PIPELINE_RESULTS[key], value)
  return value
end

local __PIPE_GET = function(key)
  local RETVAL = __PIPELINE_RESULTS[key]
  __PIPELINE_RESULTS[key] = {}
  return RETVAL
end
"""
UNPIPELINED_CODE = """
local __PIPE_ADD = function(key, value) return value end
"""
# The constants below are used when trying to identify Redis client objects
REDIS_METHODS = set(['append', 'blpop', 'brpop', 'brpoplpush', 'decr',
                     'delete', 'exists', 'expire', 'expireat', 'get', 'getbit',
                     'getset', 'hdel', 'hgetall', 'hincrby', 'hkeys', 'hlen',
                     'hmget', 'hmset', 'hset', 'hsetnx', 'hvals', 'incr',
                     'lindex', 'linsert', 'llen', 'lpop', 'lpush', 'lpushnx',
                     'lrem', 'lset', 'ltrim', 'mget', 'move', 'mset', 'mset',
                     'msetnx', 'persist', 'publish', 'randomkey', 'rename',
                     'renamenx', 'rpop', 'rpoplpush', 'rpush', 'rpushx',
                     'sadd', 'scard', 'sdiff', 'sdiffstore', 'set', 'setbit',
                     'setex', 'setnx', 'setrange', 'sinter', 'sinterstore',
                     'sismember', 'smembers', 'smove', 'sort', 'spop',
                     'srandmember', 'srem', 'strlen', 'substr', 'sunion',
                     'sunionstore', 'ttl', 'zadd', 'zcard', 'zincrby',
                     'zinterstore', 'zrange', 'zrangebyscore', 'zrank', 'zrem',
                     'zremrangebyrank', 'zrevrange' ,'zrevrangebyscore',
                     'zrevrank', 'zrevscore', 'zunionstore'])
REDIS_METHOD_COUNT = 2
REDIS_METHOD_PCT = 0.8

# A block of Lua code consisting of LuaLine objects
class LuaBlock(object):
    def __init__(self, lines=None):
        if lines is None:
            lines = []

        self.lines = lines

    @property
    def code(self):
        return ''.join(line.code for line in self.lines)

    def append(self, line):
        self.lines.append(line)

    def extend(self, block):
        self.lines.extend(line for line in block.lines)

    def __getitem__(self, *args):
        return self.lines.__getitem__(*args)

    def __len__(self):
        return len(self.lines)

    def __iter__(self):
        return iter(self.lines)

    def __str__(self):
        return ''.join(str(line) for line in self.lines)

# A line of Lua code which knows the line numbers of the corresponding Python
class LuaLine(object):
    def __init__(self, code, linenos=[], indent=0):
        self.code = code

        # Store the line numbers this code comes from
        if isinstance(linenos, list):
            self.linenos = linenos
        else:
            self.linenos = [linenos]

        self.indent = indent

    def __str__(self):
        return TAB * self.indent + self.code + '\n'

class RedisFunc(object):
    def __init__(self, func, helper=False):
        self.func = func

        # Get the source code and strip whitespace and decorators
        self.source = sully.get_func_source(func)

        # Generate the AST and do some munging to get the node that represents
        # the body of the function which is all that we care about
        self.ast = ast.parse(self.source)
        self.arg_names = [arg.id for arg in self.ast.body[0].args.args]
        self.ast = self.ast.body[0].body

        # Assume that this is a method if the first argument is self
        # This is obviously brittle, but easy and will probably work
        if self.arg_names[0] == 'self':
            self.method = True

            if not helper:
                self.client_arg = self.arg_names[1]
        else:
            self.method = False

            if not helper:
                self.client_arg = self.arg_names[0]

        # Store helper function data and constants
        self.helper = helper
        self.helper_args = []
        self.constants = {}

        # Strip the instance and client object parameters
        self.arg_names = list(self.arg_names[self.method + (not helper):])

        # Generate the code for the body of the method
        self.body = LuaBlock()
        for node in self.ast:
            block = self.process_node(node, 1 if helper else 0)
            self.body.extend(block)

        # Initialize the script to None, we'll register it Later
        self.script = None

    # Convert the value to a string representation in Lua
    def convert_value(self, value):
        if value is None:
            return 'nil'
        elif type(value) is str:
            # XXX Lua probably doesn't follow the exact same escaping rules
            #     but this will work for a lot of simple cases
            return "'" + value.encode('string_escape') + "'"
        else:
            return str(value)

    # Get the value for a constant expression
    def get_constant(self, expr):
        try:
            # Try to find this constant in the globals dictionary
            value = self.func.func_globals[expr[0]]
        except KeyError:
            # Otherwise look in the function's closure
            free_idx = self.func.func_code.co_freevars.index(expr[0])
            value = self.func.func_closure[free_idx].cell_contents

        if len(expr) > 1:
            value = getattr(value, expr[1])

        return self.convert_value(value)

    # Generate code for a single node at a particular indentation level
    def process_node(self, node, indent=0):
        code = []

        if isinstance(node, (ast.Expr, ast.Index)):
            code.append(self.process_node(node.value, indent))
        elif isinstance(node, ast.Assign):
            value = self.process_node(node.value, indent).code

            for var in node.targets:
                line = 'local %s = %s;' % (var.id, value)
                code.append(LuaLine(line, node.lineno, indent))
        elif isinstance(node, ast.List):
            line = '{' + \
                ', '.join(self.process_node(n).code for n in node.elts) + '}'
            code.append(LuaLine(line, [n.lineno for n in node.elts], indent))
        elif isinstance(node, ast.Return):
            line = 'return ' + self.process_node(node.value).code
            code.append(LuaLine(line, node.lineno, indent))
        elif isinstance(node, ast.Name):
            # Replace common constants (assuming they are not redefined)
            if node.id == 'None':
                name = 'nil'
            elif node.id in ('True', 'False'):
                name = node.id.lower()

            # Uppercase names are assumed to be constants
            elif node.id.isupper():
                name = self.get_constant((node.id,))

            # Otherwise we assume a local variable
            else:
                name = node.id

            code.append(LuaLine(name, node.lineno, indent))
        elif isinstance(node, ast.For):
            # Get the list we are looping over
            for_list = self.process_node(node.iter).code

            # Try to find a comma in the list
            try:
                comma_index = for_list.index(',')
            except ValueError:
                comma_index = False

            # This is a dumb heuristic and we just propagate this information
            # if we were more careful, but we check for a digit followed by
            # a comma to see if this is a loop over a range or a list
            if comma_index is not False and for_list[0:comma_index].isdigit():
                line = 'for %s=%s do' % (node.target.id, for_list)
            else:
                line = 'for _, %s in ipairs(%s) do' % \
                        (node.target.id, for_list)

            code.append(LuaLine(line, node.lineno, indent))
            for n in node.body:
                code.append(self.process_node(n, indent + 1))
            code.append(LuaLine('end', [], indent))
        elif isinstance(node, ast.BoolOp):
            values = ['(' + self.process_node(n).code + ')'
                      for n in node.values]

            if isinstance(node.op, ast.Or):
                op = ' or '
            elif isinstance(node.op, ast.And):
                op = ' and '
            else:
                # XXX Some unhandled operator
                print(node.op)
                raise Exception()

            code.append(LuaLine(op.join(values), node.lineno, indent))
        elif isinstance(node, ast.UnaryOp):
            if isinstance(node.op, ast.USub):
                op = '-'
            else:
                # XXX Some unhandled operator
                print(node.op)
                raise Exception()

            line = op + self.process_node(node.operand).code
            code.append(LuaLine(line, node.lineno, indent))
        elif isinstance(node, ast.BinOp):
            op1 = self.process_node(node.left).code
            op2 = self.process_node(node.right).code

            if isinstance(node.op, ast.Add):
                # Guess if either operand is a number
                if op1.isdigit() or op2.isdigit():
                    op = ' + '
                else:
                    op = ' .. '
            elif isinstance(node.op, ast.Sub):
                op = ' - '
            elif isinstance(node.op, ast.Mod):
                op = ' % '
            elif isinstance(node.op, ast.Mult):
                op = ' * '
            elif isinstance(node.op, ast.Div):
                op = ' / '
            elif isinstance(node.op, ast.Pow):
                op = ' ^ '
            else:
                # XXX Some unhandled operator
                print(node.op)
                raise Exception()

            line = op1 + op + op2
            code.append(LuaLine(line, node.lineno, indent))
        elif isinstance(node, ast.Str):
            line = self.convert_value(node.s)
            code.append(LuaLine(line, node.lineno, indent))
        elif isinstance(node, ast.Num):
            line = self.convert_value(node.n)
            code.append(LuaLine(line, node.lineno, indent))
        elif isinstance(node, ast.Call):
            # We don't support positional or keyword arguments
            if node.starargs or node.kwargs:
                raise Exception()

            raw_args = [self.process_node(n) for n in node.args]
            args = ', '.join(arg.code for arg in raw_args)

            # Handle some built-in functions
            if isinstance(node.func, ast.Name):
                if node.func.id == 'int':
                    line = 'tonumber(%s)' % args
                elif node.func.id == 'str':
                    line = 'tostring(%s)' % args
                elif node.func.id in ('range', 'xrange'):
                    # Extend to always use three arguments
                    if len(node.args) == 1:
                        args = '0, %s - 1, 1' % args
                    elif len(node.args) == 2:
                        args += ' - 1, 1'

                    line = args
                else:
                    # XXX We don't know how to handle this function
                    raise Exception()

            # XXX We assume now that the function being called is an Attribute

            # Perform string replacement
            elif node.func.attr == 'replace':
                line = 'string.gsub(%s, %s)\n' \
                        % (self.process_node(node.func.value).code, args)

            # Join a table of strings
            elif node.func.attr == 'join':
                line = 'table.concat(%s, %s)\n' \
                        % (args, self.process_node(node.func.value).code)

            # If we're calling append, add to the end of a list
            elif node.func.attr == 'append':
                line = 'table.insert(%s, %s)' \
                        % (self.process_node(node.func.value).code, args)

            # If we're calling insert, add to the appropriate list position
            elif node.func.attr == 'insert':
                line = 'table.insert(%s, %s + 1, %s)\n' \
                        % (self.process_node(node.func.value).code,
                           raw_args[0].code, raw_args[1].code)

            # Check if we have a method call
            elif node.func.value.id == 'self':
                # Add this function like a new argument
                new_arg = SELF_ARG + node.func.attr
                if new_arg not in self.arg_names:
                    self.arg_names.append(new_arg)

                line = '%s(%s)' % (new_arg, args)

            # XXX Assume this is a Redis pipeline execution
            elif node.func.attr == 'pipe':
                # Do nothing to start a pipeline
                line = ''
            elif node.func.attr == 'execute':
                expr = self.process_node(node.func.value).code
                line = '__PIPE_GET(\'%s\')' % expr

            # XXX Otherwise, assume this is a redis function call
            else:
                # Generate the Redis function call expression
                call = 'redis.call(\'%s\', %s)' % (node.func.attr, args)

                # Wrap the Redis call in a function which stores the
                # result if needed later for pipelining and returns it
                expr = self.process_node(node.func.value).code
                line = '__PIPE_ADD(\'%s\', %s)' % (expr, call)

            if line:
                code.append(LuaLine(line, node.lineno, indent))
        elif isinstance(node, ast.If):
            # Add a line for the initial test
            test = self.process_node(node.test).code
            line = 'if %s then' % test
            code.append(LuaLine(line, node.lineno, indent))

            # Generate the body of the if block
            for n in node.body:
                code.append(self.process_node(n, indent + 1))

            # Generate the body of the else branch
            if len(node.orelse) > 0:
                code.append(LuaLine('else', [], indent))
            for n in node.orelse:
                code.append(self.process_node(n, indent + 1))

            # Close the if block
            code.append(LuaLine('end', [], indent))
        elif isinstance(node, ast.Compare):
            # XXX We only handle a single comparison
            if len(node.ops) != 1 or len(node.comparators) != 1:
                raise Exception()

            lhs = self.process_node(node.left).code

            if isinstance(node.ops[0], ast.Eq):
                op = ' == '
            elif isinstance(node.ops[0], ast.NotEq):
                op = ' != '
            else:
                # XXX We don't handle this type of comparison
                print(node.ops[0])
                raise Exception()

            rhs = self.process_node(node.comparators[0]).code
            line = '%s %s %s' % (lhs, op, rhs)
            code.append(LuaLine(line, node.lineno, indent))
        elif isinstance(node, ast.Attribute):
            obj = self.process_node(node.value).code

            # XXX Assume uppercase values are constants
            if obj != 'self' and not node.attr.isupper():
                # XXX We're probably doing some external stuff we can't handle
                raise Exception()

            if obj == 'self':
                # Add a new argument to handle this
                expr = SELF_ARG + node.attr
                if expr not in self.arg_names:
                    if self.helper:
                        self.helper_args.append(expr)
                    else:
                        self.arg_names.append(expr)
            else:
                expr = self.get_constant((obj, node.attr))

            code.append(LuaLine(expr, node.lineno, indent))
        elif isinstance(node, ast.Subscript):
            subs = self.process_node(node.slice).code
            expr = self.process_node(node.value).code

            # Here we check the __DICT property of the object to see if
            # it is not a dictionary in which case we add 1 to the index
            line = '%s[(%s.__DICT) and (%s) or (%s + 1)]' % \
                    (expr, expr, subs, subs)

            code.append(LuaLine(line, node.lineno, indent))
        elif isinstance(node, ast.Print):
            # We only handle prints to stdout
            if node.dest is not None:
                raise Exception()

            # Add a log statement for each print
            for value in node.values:
                value = self.process_node(value)
                line = 'redis.log(redis.LOG_DEBUG, %s)' % value
                code.append(LuaLine(line, node.lineno, indent))
        else:
            # XXX This type of node is not handled
            print(ast.dump(node))
            print(node._fields)
            raise Exception()

        return LuaBlock(code)

    # Generate code to unpack arguments with their correct name and type
    def unpack_args(self, args, arg_names, start_arg=0, method_self=None):
        # Unpack arguments to their original names performing
        # any necessary type conversions
        # XXX We assume arguments will always have the same type
        arg_unpacking = ''
        new_args = 0
        for i, name in enumerate(arg_names[start_arg:]):
            # Perform the lookup for class variables
            # We should be able to extend this to support multiple lookups
            # i.e., self.foo.bar
            if name.startswith(SELF_ARG):
                arg = getattr(method_self, name[len(SELF_ARG):])
            else:
                arg = args[i + start_arg]

            # Generate code for methods called within this method
            if isinstance(arg, types.MethodType):
                # XXX We currently assume that methods called by the method
                #     we're translating do not access any attributes of
                #     the instance
                wrapped = RedisFunc(arg, helper=True)

                # Add any new args which come from the current object
                # to the list of arguments for helper functions
                for new_arg in wrapped.helper_args:
                    if new_arg not in arg_names:
                        arg_names.append(new_arg)
                        new_args += 1

                # Dump the helper function code into a local variable
                arg_unpacking += 'local %s = function(%s)\n%s\nend\n' % \
                        (self.arg_names[i + start_arg],
                         ', '.join(wrapped.arg_names), wrapped.body)
                continue

            # Convert numbers from string form
            elif isinstance(arg, (int, long, float)):
                conversion = 'tonumber'
            elif isinstance(arg, PACKED_TYPES):
                conversion = 'cmsgpack.unpack'
            else:
                conversion = ''

            arg_unpacking += 'local %s = %s(ARGV[%d])\n' % \
                    (arg_names[i + start_arg], conversion ,i + start_arg + 1)

            # Track if this is a dictionary so we know if we
            # need to add one to indexes into the Lua table
            if isinstance(arg, dict):
                arg_unpacking += '%s.__DICT = true\n' % \
                        arg_names[i + start_arg]

        # Expand any necessary helper arguments
        if new_args > 0:
            # Args is passed through as an empty array since all of them
            # must be pulled from method_self anyway
            helper_unpacking = self.unpack_args([], arg_names,
                                                start_arg + new_args,
                                                method_self)
        else:
            helper_unpacking = ''

        return helper_unpacking + arg_unpacking


    # Register the script with the backend
    def register_script(self, client, args, method_self=None):
        body = str(self.body)

        # XXX This is dumb but lets us avoid most of the pipelining
        #     overhead if we're sure that it isn't needed
        pipeline_code = PIPELINED_CODE if '__PIPE_GET' in body \
                                       else UNPIPELINED_CODE

        arg_unpacking = self.unpack_args(args, self.arg_names, 0, method_self)
        code = pipeline_code + arg_unpacking + body
        print(code)

        self.script = client.register_script(code)

    def __get__(self, instance, owner):
        # We need a descriptor here to get the class instance then we
        # just stick it as the first argument we pass to __call__
        def inner(*args):
            return self.__call__(instance, *args)

        return inner

    def __call__(self, *args):
        # Check if this is a method and pull the correct arguments
        if self.method:
            method_self = args[0]
            client = args[1]
            args = list(args[2:])
        else:
            method_self = None
            client = args[0]
            args = list(args[1:])

        # Register the script if necessary
        # We need to pass the arguments so we can do type conversions
        if self.script is None:
            self.register_script(client, args, method_self)

        # Add arguments which are pulled from the class instance
        args += [getattr(method_self, attr[len(SELF_ARG):]) \
                for attr in self.arg_names \
                if attr.startswith(SELF_ARG)]

        # Dump the necessary arguments with msgpack
        for i in range(len(args)):
            if isinstance(args[i], PACKED_TYPES):
                args[i] = msgpack.dumps(args[i])

        return self.script(args=args)

redis_server = RedisFunc

def identify_redis(func):
    redis_func_objs = []
    nonredis_func_objs = []
    func_ast = sully.get_func_ast(func)
    node_walkers = (ast.walk(func_node) for func_node in func_ast)
    for node in itertools.chain.from_iterable(node_walkers):
        # Skip any nodes which are not function calls on objects
        if not (isinstance(node, ast.Call) and
                isinstance(node.func, ast.Attribute)):
            continue

        # Record all function calls
        if node.func.attr in REDIS_METHODS:
            redis_func_objs.append(node.func.value)
        else:
            nonredis_func_objs.append(node.func.value)

    # Loop through all the found function objects to pick
    # out the ones we deem to represent Redis interfaces
    redis_objs = []
    while len(redis_func_objs) > 0:
        obj = redis_func_objs.pop()

        # Remove all call nodes matching this object
        redis_before = len(redis_func_objs) + 1
        redis_func_objs = [obj2 for obj2 in redis_func_objs
                if not sully.nodes_equal(obj, obj2)]

        nonredis_before = len(nonredis_func_objs)
        nonredis_func_objs = [obj2 for obj2 in nonredis_func_objs
                if not sully.nodes_equal(obj, obj2)]

        # If the object meets a threshold of calls for the object
        # and a certain percentage of all calls match, record it
        redis_calls = redis_before - len(redis_func_objs)
        nonredis_calls = nonredis_before - len(nonredis_func_objs)
        if redis_calls >= REDIS_METHOD_COUNT and \
           (redis_calls * 1.0 /
                   (redis_calls + nonredis_calls)) >= REDIS_METHOD_PCT:
           redis_objs.append(obj)

    return redis_objs
