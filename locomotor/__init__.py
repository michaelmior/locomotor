import ast
import byteplay
import collections
import copy
import functools
import hashlib
import inspect
import itertools
import msgpack
import sully
import types

TAB = '  '
SELF_ARG = 'SELF__'
PACKED_TYPES = (list, dict)
LUA_HEADER = """
local __RETVAL = function(value, retval)
  local __RESULT = {}
  __RESULT["__value"] = value
  __RESULT["__return"] = retval

  return cmsgpack.pack(__RESULT)
end
"""
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
                     'delete', 'execute', 'exists', 'expire', 'expireat',
                     'get', 'getbit', 'getset', 'hdel', 'hget', 'hgetall',
                     'hincrby', 'hkeys', 'hlen', 'hmget', 'hmset', 'hset',
                     'hsetnx', 'hvals', 'incr', 'lindex', 'linsert', 'llen',
                     'lpop', 'lpush', 'lpushnx', 'lrange', 'lrem', 'lset',
                     'ltrim', 'mget', 'move', 'mset', 'mset', 'msetnx',
                     'persist', 'publish', 'randomkey', 'rename', 'renamenx',
                     'rpop', 'rpoplpush', 'rpush', 'rpushx', 'sadd', 'scard',
                     'sdiff', 'sdiffstore', 'set', 'setbit', 'setex', 'setnx',
                     'setrange', 'sinter', 'sinterstore', 'sismember',
                     'smembers', 'smove', 'sort', 'spop', 'srandmember',
                     'srem', 'strlen', 'substr', 'sunion', 'sunionstore',
                     'ttl', 'zadd', 'zcard', 'zincrby', 'zinterstore',
                     'zrange', 'zrangebyscore', 'zrank', 'zrem',
                     'zremrangebyrank', 'zrevrange', 'zrevrangebyscore',
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
        for line in block.lines:
            self.append(line)

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
    def __init__(self, code, node=None, indent=0):
        self.code = code
        self.node = node
        self.indent = indent

    def __str__(self):
        return TAB * self.indent + self.code + '\n'

class ScriptRegistry(object):
    SCRIPTS = {}

    # Register the script and return its ID
    @classmethod
    def register_script(cls, client, lua_code):
        script = client.register_script(lua_code)
        script_id = hashlib.md5(lua_code).hexdigest()
        cls.SCRIPTS[(client, script_id)] = script
        return script_id

    # Execute a pre-registered script
    @classmethod
    def run_script(cls, client, script_id, args):
        # Dump the necessary arguments with msgpack
        for i in range(len(args)):
            if isinstance(args[i], PACKED_TYPES):
                args[i] = msgpack.dumps(args[i])

        # Get the registered script
        script = cls.SCRIPTS[(client, script_id)]

        # Execute the script and unpack the return value
        retval = script(args=args)
        if retval is None:
            retval = {'__return': True}
        else:
            retval = msgpack.loads(retval)

        # Specify an empty value if none is given
        if '__value' not in retval:
            retval['__value'] = None

        return retval

class RedisFuncFragment(object):
    def __init__(self, taint, minlineno=None, maxlineno=None,
                 redis_objs=None, helper=False):
        self.taint = taint
        if redis_objs:
            self.redis_objs = redis_objs
        else:
            self.redis_objs = identify_redis_objs(taint.func)

        # Fail if we don't have a valid Redis object
        if len(self.redis_objs) == 0 and not helper:
            raise Exception()

        # Get argument names
        body_ast = self.taint.func_ast.body[0]
        self.arg_names = [arg.id for arg in body_ast.args.args]

        # Assume that this is a method if the first argument is self
        # This is obviously brittle, but easy and will probably work
        self.method = self.arg_names[0] == 'self'

        # Strip the instance and client object parameters
        self.arg_names = list(self.arg_names[self.method + (not helper):])
        self.helpers = self.taint.functions_in_range(None, None)

        # Get the expressions we need to bring in and out of this block
        if not minlineno:
            minlineno = body_ast.minlineno
        if not maxlineno:
            maxlineno = body_ast.maxlineno
        self.in_exprs, self.out_exprs = sully.block_inout(self.taint.func_ast,
                                                          minlineno, maxlineno)
        self.minlineno = minlineno
        self.maxlineno = maxlineno

        # Rename the expressions to change things like
        # (self, foo) to SELF__foo
        self.in_exprs.difference_update(self.arg_names)
        self.in_exprs = self.arg_names + self.rename_expressions(self.in_exprs)
        for obj in self.redis_objs:
            if isinstance(obj, ast.Name) and obj.id in self.in_exprs:
                self.in_exprs.remove(obj.id)
        self.out_exprs = self.rename_expressions(self.out_exprs)

        # Store helper function data and constants
        self.helper = helper
        self.constants = {}

        # Generate the code for the body of the method
        self.body = LuaBlock()
        for node in self.taint.func_ast.body[0].body:
            # Ignore lines we don't want to translate
            if node.lineno < self.minlineno:
                continue
            if node.lineno > self.maxlineno:
                break

            block = self.process_node(node, 1 if helper else 0)
            self.body.extend(block)

        # Initialize the script ID to None, we'll register it Later
        self.script_id = None

    # Rename all expressions in a list to their appropriate names
    def rename_expressions(self, expressions):
        outlist = []
        for expr in expressions:
            if isinstance(expr, tuple):
                if expr[0] == 'self':
                    outlist.append(SELF_ARG + expr[1])
                elif expr[1].upper():
                    # Delete constants we don't need
                    continue
                elif not expr[1].upper():
                    # XXX This value is not supported
                    raise Exception()
            else:
                outlist.append(expr)

        return outlist

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
            value = self.taint.func.func_globals[expr[0]]
        except KeyError:
            # Otherwise look in the function's closure
            free_idx = self.taint.func.func_code.co_freevars.index(expr[0])
            value = self.taint.func.func_closure[free_idx].cell_contents

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
                code.append(LuaLine(line, node, indent))
        elif isinstance(node, ast.List):
            line = '{' + \
                ', '.join(self.process_node(n).code for n in node.elts) + '}'
            code.append(LuaLine(line, node, indent))
        elif isinstance(node, ast.Return):
            retval = self.process_node(node.value).code

            # If this is the final return value, pack it up with cmsgpack
            if self.helper:
                line = 'return %s' % retval
            else:
                line = 'return __RETVAL(%s, true)' % retval

            code.append(LuaLine(line, node, indent))
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

            code.append(LuaLine(name, node, indent))
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

            code.append(LuaLine(line, node, indent))
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

            code.append(LuaLine(op.join(values), node, indent))
        elif isinstance(node, ast.UnaryOp):
            if isinstance(node.op, ast.USub):
                op = '-'
            else:
                # XXX Some unhandled operator
                print(node.op)
                raise Exception()

            line = op + self.process_node(node.operand).code
            code.append(LuaLine(line, node, indent))
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
            code.append(LuaLine(line, node, indent))
        elif isinstance(node, ast.Str):
            line = self.convert_value(node.s)
            code.append(LuaLine(line, node, indent))
        elif isinstance(node, ast.Num):
            line = self.convert_value(node.n)
            code.append(LuaLine(line, node, indent))
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
            # XXX This is somewhat annoying but we have to wrap this up in an
            #     anonymous function to avoid accidentally capturing the second
            #     value produced by gsub when this expression is used later
            elif node.func.attr == 'replace':
                line = '((function() local __TEMP, _; ' \
                       '__TEMP, _ = string.gsub(%s, %s); ' \
                       'return __TEMP end)())' \
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
                line = '%s(%s)' % (SELF_ARG + node.func.attr, args)

            # XXX Assume this is a Redis pipeline execution
            elif node.func.attr == 'pipe':
                # Do nothing to start a pipeline
                line = ''
            elif node.func.attr == 'execute':
                expr = self.process_node(node.func.value).code
                line = '__PIPE_GET(\'%s\')' % expr

            # XXX Otherwise, assume this is a redis function call
            elif any(sully.nodes_equal(node.func.value, obj)
                    for obj in self.redis_objs):
                # Generate the Redis function call expression
                call = 'redis.call(\'%s\', %s)' % (node.func.attr, args)

                # Wrap the Redis call in a function which stores the
                # result if needed later for pipelining and returns it
                expr = self.process_node(node.func.value).code
                line = '__PIPE_ADD(\'%s\', %s)' % (expr, call)
            else:
                # XXX Something we can't handle
                print(ast.dump(node))
                raise Exception()

            if line:
                code.append(LuaLine(line, node, indent))
        elif isinstance(node, ast.If):
            # Add a line for the initial test
            test = self.process_node(node.test).code
            line = 'if %s then' % test
            code.append(LuaLine(line, node, indent))

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
            code.append(LuaLine(line, node, indent))
        elif isinstance(node, ast.Attribute):
            obj = self.process_node(node.value).code

            # XXX Assume uppercase values are constants
            if obj != 'self' and not node.attr.isupper():
                # XXX We're probably doing some external stuff we can't handle
                raise Exception()

            if obj == 'self':
                # Use a new argument to handle this
                expr = SELF_ARG + node.attr
            else:
                expr = self.get_constant((obj, node.attr))

            code.append(LuaLine(expr, node, indent))
        elif isinstance(node, ast.Subscript):
            subs = self.process_node(node.slice).code
            expr = self.process_node(node.value).code

            # Here we check the __DICT property of the object to see if
            # it is not a dictionary in which case we add 1 to the index
            line = '%s[(%s.__DICT) and (%s) or (%s + 1)]' % \
                    (expr, expr, subs, subs)

            code.append(LuaLine(line, node, indent))
        elif isinstance(node, ast.Print):
            # We only handle prints to stdout
            if node.dest is not None:
                raise Exception()

            # Add a log statement for each print
            for value in node.values:
                value = self.process_node(value)
                line = 'redis.log(redis.LOG_DEBUG, %s)' % value
                code.append(LuaLine(line, node, indent))
        elif isinstance(node, ast.Pass):
            line = 'do end'
            code.append(LuaLine(line, node, indent))
        else:
            # XXX This type of node is not handled
            print(ast.dump(node))
            print(node._fields)
            raise Exception()

        return LuaBlock(code)

    # Generate code to unpack arguments with their correct name and type
    def unpack_args(self, args, start_arg=0, helpers=[],
                    method_self=None):
        # Unpack arguments to their original names performing
        # any necessary type conversions
        # XXX We assume arguments will always have the same type
        arg_unpacking = ''
        new_args = 0

        # Generate code for all helper functions
        helper_functions = ''
        for method_name in helpers:
            # We only need to deal with helper functions
            if method_name[0] != 'self':
                continue

            # XXX We currently assume that methods called by the method
            #     we're translating do not access any attributes of
            #     the instance
            method = getattr(method_self, method_name[1])
            taint = sully.TaintAnalysis(method)
            wrapped = RedisFuncFragment(taint, helper=True)

            # Add any newly discovered expressions which are required
            for in_expr in wrapped.in_exprs:
                if in_expr not in wrapped.arg_names and \
                   in_expr not in self.in_exprs:
                    self.in_exprs.append(in_expr)
                    new_args += 1

            # Dump the helper function code into a local variable
            helper_functions += 'local %s = function(%s)\n%s\nend\n' % \
                    (SELF_ARG + method_name[1],
                            ', '.join(wrapped.arg_names), wrapped.body)

        for i, name in enumerate(self.in_exprs[start_arg:]):
            # Perform the lookup for class variables
            # We should be able to extend this to support multiple lookups
            # i.e., self.foo.bar
            if name.startswith(SELF_ARG):
                arg = getattr(method_self, name[len(SELF_ARG):])
            else:
                arg = args[i + start_arg]

            if isinstance(arg, (int, long, float)):
                # Convert numbers from string form
                conversion = 'tonumber'
            elif isinstance(arg, PACKED_TYPES):
                conversion = 'cmsgpack.unpack'
            else:
                conversion = ''

            arg_unpacking += 'local %s = %s(ARGV[%d])\n' % \
                    (self.in_exprs[i + start_arg], conversion ,i + start_arg + 1)

            # Track if this is a dictionary so we know if we
            # need to add one to indexes into the Lua table
            if isinstance(arg, dict):
                arg_unpacking += '%s.__DICT = true\n' % \
                        self.in_exprs[i + start_arg]

        # Expand any necessary helper arguments
        if new_args > 0:
            # Args is passed through as an empty array since all of them
            # must be pulled from method_self anyway
            helper_unpacking = self.unpack_args([], start_arg + new_args, [],
                                                method_self)
        else:
            helper_unpacking = ''

        return helper_unpacking + arg_unpacking + helper_functions


    # Produce the lua code for this script fragment
    def lua_code(self, client, args, method_self=None):
        body = str(self.body)

        # XXX This is dumb but lets us avoid most of the pipelining
        #     overhead if we're sure that it isn't needed
        pipeline_code = PIPELINED_CODE if '__PIPE_GET' in body \
                                       else UNPIPELINED_CODE

        arg_unpacking = self.unpack_args(args, 0, self.helpers, method_self)
        return LUA_HEADER + pipeline_code + arg_unpacking + body

    def __get__(self, instance, owner):
        # We need a descriptor here to get the class instance then we
        # just stick it as the first argument we pass to __call__
        @functools.wraps(self.taint.func)
        def inner(*args):
            return self.__call__(instance, *args)

        return inner

    def __call__(self, *args):
        # Save the original arguments so we can use them when calling later
        orig_args = copy.copy(args)

        # Check if this is a method and pull the correct arguments
        if self.method:
            method_self = args[0]
            client = args[1]
            args = list(args[2:])
        else:
            method_self = None
            client = args[0]
            args = list(args[1:])

        # Register this script if needed
        if self.script_id is None:
            lua_code = self.lua_code(client, args, method_self)
            self.script_id = ScriptRegistry.register_script(client, lua_code)

            # Get all the arguments to go to the function
            arg_exprs = copy.copy(self.arg_names)
            for attr in self.in_exprs:
                if attr.startswith(SELF_ARG):
                    arg_exprs.append('self.%s' % attr[len(SELF_ARG):])

            # XXX For now, there can be only one
            client_arg = self.redis_objs[0].id

            # Compile code to call our script
            # We first store the return value of the script in a temporary
            # variable and then check if we're supposed to return
            # __RETURN_HERE__ is a placeholder that we can insert the return
            # instruction as "return" is not valid in the current context
            script_call = '__RETVAL = ScriptRegistry.run_script' \
                          '(%s, "%s", [%s])\n' \
                    % (client_arg, self.script_id, ', '.join(arg_exprs))
            script_call += 'if __RETVAL["__return"]:\n' \
                           '    __RETVAL["__value"]\n' \
                           '    __RETURN_HERE__\n' \
                           'for __var, __value in __RETVAL.iteritems():\n' \
                           '    if not __var.startswith("__"):\n' \
                           '        locals()[__var] == __value'

            script_call = compile(script_call, '<string>', 'exec')

            # Replace LOAD_NAME with LOAD_FAST for all function argument
            # And store where we need to splice in the return instruction
            new_code = byteplay.Code.from_code(script_call)
            linenos = []
            for i, instr in enumerate(new_code.code):
                if instr[0] == byteplay.LOAD_NAME and \
                        instr[1] in self.taint.func.func_code.co_varnames:
                    new_code.code[i] = (byteplay.LOAD_FAST, instr[1])

                # Find where our return instruction should go
                if instr[0] == byteplay.LOAD_NAME \
                        and instr[1] == '__RETURN_HERE__':
                    return_loc = i - len(linenos)

                # Track where line numbers appear so we can remove them
                # to retain the line numbers from the original function
                if instr[0] == byteplay.SetLineno:
                    linenos.append(i)

            # Remove all line number markers
            removed_lines = 0
            for lineno in linenos:
                del new_code.code[lineno - removed_lines]
                removed_lines += 1

            # Patch in the return instruction
            new_code.code[return_loc-1:return_loc+2] = \
                    [(byteplay.RETURN_VALUE, None)]

            # Copy the line number so the first line matches
            code = byteplay.Code.from_code(self.taint.func.func_code)

            # Find the start and line lines where we need to patch in
            firstline = code.code[0][1] - 2
            startline = endline = None
            for i, instr in enumerate(code.code):
                if startline is None and instr[0] == byteplay.SetLineno and \
                        (instr[1] - firstline) >= self.minlineno:
                    startline = i + 1
                if instr[0] == byteplay.SetLineno and \
                        (instr[1] - firstline) <= self.maxlineno:
                    endline = i - 1

                # If we haven't found the end, keep advancing
                if instr[0] == byteplay.SetLineno and \
                        (instr[1] - firstline) <= self.maxlineno:
                    endline = i

            # Patch this into the original function
            # We skip the first line since this is an unwanted SetLineno
            code.code[startline:endline] = new_code.code
            self.taint.func.func_code = code.to_code()

            # Make the ScriptRegistry global available
            self.taint.func.func_globals['ScriptRegistry'] = ScriptRegistry

        return self.taint.func(*orig_args)

# Create a function decorator which converts a function to run on the server
def redis_server(method=None, redis_objs=None, minlineno=None, maxlineno=None):
    def decorator(method):
        taint = sully.TaintAnalysis(method)
        fragment = RedisFuncFragment(taint, redis_objs=redis_objs,
                                            minlineno=minlineno,
                                            maxlineno=maxlineno)
        return functools.update_wrapper(fragment, method)

    return decorator(method) if method else decorator

def identify_redis_objs(func):
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

# Identify functions in a class or module which use Redis
def identify_redis_funcs(cls_or_mod):
    redis_funcs = {}

    for obj in dir(cls_or_mod):
        # Skip things which look private
        if obj.startswith('_'):
            continue

        val = getattr(cls_or_mod, obj)

        if isinstance(val, (types.ClassType)):
            # Recursively check all classes
            class_funcs = identify_redis_funcs(cls_or_mod)
            redis_funcs.update(class_funcs)
        elif isinstance(val, (types.FunctionType, types.MethodType)):
            # Identify Redis objects within the function
            objs = identify_redis_objs(val)
            if len(objs) > 0:
                redis_funcs[val] = objs

    return redis_funcs
