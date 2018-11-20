import ast
import byteplay
import collections
import copy
import datetime
import functools
import hashlib
import inspect
import msgpack
import os
import re
import redis
import sully
import time

from .identify import *

#: The string literal to use for tabs in generated Lua code
TAB = '  '

#: Types which should be serialized via msgpack
PACKED_TYPES = (list, dict, types.NoneType, datetime.datetime)

#: A header added to all generated Lua code
LUA_HEADER = open(os.path.dirname(__file__) + '/lua/header.lua').read()

#: Additional functions for code which uses pipelining
PIPELINED_CODE = open(os.path.dirname(__file__) + '/lua/pipelined.lua').read()

#: A dummy function to use for code which does not involve pipelining
UNPIPELINED_CODE = """
local __PIPE_ADD = function(key, value) return value end
"""

#: Function names which we assume are builtins
FUNC_BUILTINS = ('append', 'insert', 'join', 'replace')

#: A channel used to push debug messages from Lua scripts
DEBUG_LOG_CHANNEL = 'locomotor-debug'

#: A flag to control Lua script debug messages
# XXX This may result in changed behaviour since printing expressions
#     currently may have side effects but this should be fixable
LUA_DEBUG = False

#: The class used for pipelined operations
# This was updated in v3 of the Redis Python library
PIPELINE_CLASS = getattr(redis.client, 'BasePipeline', redis.client.Pipeline)


def decode_msgpack(obj):
    # TODO: Convert datetime objects back
    return obj


def encode_msgpack(obj):
    if isinstance(obj, datetime.datetime):
        return time.mktime(obj.timetuple())
    return obj


# A block of Lua code consisting of LuaLine objects
class LuaBlock(object):
    def __init__(self, lines=None):
        if lines is None:
            lines = []

        self.lines = []
        self.names = set()
        for line in lines:
            self.append(line)

    @property
    def code(self):
        return ''.join(line.code for line in self.lines)

    def append(self, line):
        if not line:
            return

        self.lines.append(line)
        self.names.update(line.names)
        line.names = set()

    def extend(self, block):
        for line in block.lines:
            if not line:
                continue

            self.append(line)

        self.names.update(block.names)

    def __getitem__(self, *args):
        return self.lines.__getitem__(*args)

    def __len__(self):
        return len(self.lines)

    def __iter__(self):
        return iter(self.lines)

    def __repr__(self):
        return '(' + repr(list(self.names)) + ', ' + \
               '[' + ''.join(repr(line) for line in self.lines) + ']' + ')'

    def __str__(self):
        if len(self.names) > 0:
            code = '\n'.join('local %s;' % name for name in self.names) + '\n'
        else:
            code = ''

        code += ''.join(str(line) for line in self.lines)
        return code


# A line of Lua code which knows the line numbers of the corresponding Python
class LuaLine(object):
    def __init__(self, code, node=None, indent=0, names=set()):
        self.code = code
        self.node = node
        self.indent = indent
        self.names = names

    @staticmethod
    def debug(message, *args):
        if LUA_DEBUG:
            # Escape quotes in the message
            message = message.replace("'", "\\'")

            # Format using arguments if provided
            if len(args) > 0:
                message = "string.format('%s', %s)" % \
                          (message, ', '.join('tostring(%s)' % arg
                                              for arg in args))
            else:
                message = "'%s'" % message

            # Publish the message to the debug channel
            return LuaLine("redis.call('publish', '%s', %s);" %
                           (DEBUG_LOG_CHANNEL, message))
        else:
            return None

    def __repr__(self):
        return repr(str(self))

    def __str__(self):
        return TAB * self.indent + self.code + '\n'


class ScriptRegistry(object):
    SCRIPTS = {}

    # Register the script and return its ID
    @classmethod
    def register_script(cls, client, lua_code):
        script = client.register_script(lua_code)
        script_id = hashlib.md5(lua_code).hexdigest()
        cls.SCRIPTS[script_id] = script
        return script_id

    # Execute a pre-registered script
    @classmethod
    def run_script(cls, client, script_id, args):
        # Dump the necessary arguments with msgpack
        for i in range(len(args)):
            if isinstance(args[i], PACKED_TYPES):
                args[i] = msgpack.packb(args[i], default=encode_msgpack)

        # Get the registered script
        script = cls.SCRIPTS[script_id]

        # Ensure the script is loaded for pipelining
        # XXX This makes assumptions on the client library
        if isinstance(client, PIPELINE_CLASS):
            cmd_exec = client.immediate_execute_command
        else:
            cmd_exec = client.execute_command

        if not script.sha or not cmd_exec('SCRIPT', 'EXISTS', script.sha,
                                          **{'parse': 'EXISTS'})[0]:
            script.sha = cmd_exec('SCRIPT', 'LOAD', script.script,
                                  **{'parse': 'LOAD'})

        # Execute the script and unpack the return value
        retval = cmd_exec('EVALSHA', script.sha, 0, *args)

        if retval is None:
            retval = {'__return': True}
        else:
            retval = msgpack.unpackb(retval, object_hook=decode_msgpack)

        # Specify an empty value if none is given
        if '__value' not in retval:
            retval['__value'] = None

        return retval


class UntranslatableCodeException(Exception):
    """Exception raised when code can't be translated"""

    def __init__(self, node):
        self.node = node
        message = ast.dump(node)
        super(UntranslatableCodeException, self).__init__(message)

class RedisFuncFragment(object):
    def __init__(self, taint, minlineno=None, maxlineno=None,
                 redis_objs=None, helper=False):
        self.taint = taint
        if redis_objs:
            self.redis_objs = []

            for obj in redis_objs:
                # Allow specification as bare names
                if isinstance(obj, str):
                    obj = ast.Name(id=obj, ctx=ast.Load())

                self.redis_objs.append(obj)
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
        self.arg_names = list(self.arg_names[self.method:])
        for obj in self.redis_objs:
            self.arg_names.remove(obj.id)

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

        # Translate the expressions to a more useful format
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

    def rename_expressions(self, expressions):
        """Rename all expressions in a list to their appropriate names"""

        outlist = []
        for expr in expressions:
            if isinstance(expr, tuple):
                if expr[0] == 'self':
                    # Keep hold of any attributes of self we need
                    outlist.append(expr)
                elif expr[1].upper():
                    # Delete constants we don't need
                    continue
                elif not expr[1].upper():
                    # XXX This value is not supported
                    raise Exception()
            else:
                outlist.append(expr)

        return outlist

    def convert_value(self, value):
        """Convert the value to a string representation in Lua"""

        if value is None:
            return 'nil'
        elif type(value) is str:
            # XXX Lua probably doesn't follow the exact same escaping rules
            #     but this will work for a lot of simple cases
            return "'" + value.encode('string_escape') + "'"
        else:
            return str(value)

    def get_constant(self, expr):
        """Get the value for a constant expression"""

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

    def process_node(self, node, indent=0, loops = 0):
        """Generate code for a single node at a particular indentation level"""

        code = []

        # Call the corresponding method or produce an error
        try:
            cls = node.__class__.__name__
            getattr(self, 'process_' + cls)(node, code, indent, loops)
        except KeyError:
            # XXX This type of node is not handled
            raise UntranslatableCodeException(node)

        return LuaBlock(code)

    def process_Assign(self, node, code, indent, loops):
        """Generate code for an assignment operation"""
        value = self.process_node(node.value, indent, loops).code

        for var in node.targets:
            if isinstance(var, ast.Name):
                name = var.id
            elif isinstance(var, (ast.Attribute, ast.Subscript)) and \
                    isinstance(var.value, ast.Name):
                name = var.value.id
            else:
                raise UntranslatableCodeException(node)

            var = self.process_node(var).code
            names = set([name])
            line = '%s = %s;' % (var, value)
            code.append(LuaLine(line, node, indent, names))

        if LUA_DEBUG:
            code.append(LuaLine.debug('ASSIGNING %%s TO %s' %
                                      ', '.join(v.id for v in node.targets),
                                      value))

    def process_Attribute(self, node, code, indent, loops):
        """Generate code for an attribute access x.y"""

        obj = self.process_node(node.value).code

        # XXX Assume uppercase values are constants
        if obj != 'self' and not node.attr.isupper():
            # XXX We're probably doing some external stuff we can't handle
            raise UntranslatableCodeException(node)

        if obj == 'self':
            # Access the Lua table corresponding to self
            expr = 'self.' + node.attr
        else:
            expr = self.get_constant((obj, node.attr))

        code.append(LuaLine(expr, node, indent))

    def process_AugAssign(self, node, code, indent, loops):
        """Generate code for agumented assignment (e.g. +=)"""

        target = self.process_node(node.target).code
        value = self.process_node(node.value).code

        if isinstance(node.op, ast.Add):
            line = '%s = %s + %s' % (target, target, value)
        else:
            # XXX Some unhandled operator
            raise UntranslatableCodeException(node)

        code.append(LuaLine(line, node, indent))

    def process_BinOp(self, node, code, indent, loops):
        """Generate code for a binary operator"""

        op1 = self.process_node(node.left).code
        op2 = self.process_node(node.right).code

        if isinstance(node.op, ast.Add):
            int_add = False
            for op in (node.left, node.right):
                if isinstance(op, ast.Call) and isinstance(op.func, ast.Name) \
                        and op.func.id in ('int', 'float'):
                    int_add = True
                    break

            # Guess if either operand is a number
            # XXX This will fail if we add two numerical variables
            if int_add or op1.isdigit() or op2.isdigit():
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
            raise UntranslatableCodeException(node)

        line = op1 + op + op2
        code.append(LuaLine(line, node, indent))

    def process_BoolOp(self, node, code, indent, loops):
        """Generate code for a boolean operator"""

        values = ['(' + self.process_node(n).code + ')' for n in node.values]

        if isinstance(node.op, ast.Or):
            op = '__OR'
        elif isinstance(node.op, ast.And):
            op = '__AND'
        else:
            # XXX Some unhandled operator
            raise UntranslatableCodeException(node)

        line = '%s(%s)' % (op, ', '.join(values))
        code.append(LuaLine(line, node, indent))

    def process_Call(self, node, code, indent, loops):
        """Generate code for a function call"""

        # We don't support positional or keyword arguments
        if node.starargs or node.kwargs:
            raise UntranslatableCodeException(node)

        raw_args = [self.process_node(n) for n in node.args]
        args = ', '.join(arg.code for arg in raw_args)

        # Handle some built-in functions
        if isinstance(node.func, ast.Name):
            if node.func.id in ('int', 'float'):
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
            elif node.func.id == 'len':
                assert len(node.args) == 1
                line = '#' + args
            else:
                # XXX We don't know how to handle this function
                raise UntranslatableCodeException(node)

        # XXX We assume now that the function being called is an Attribute

        # Get the current time for time.time()
        elif isinstance(node.func, ast.Attribute) and \
                isinstance(node.func.value, ast.Name) and \
                node.func.value.id == node.func.attr == 'time':
            line = '((function() local __TIME = redis.call("TIME"); ' \
                   'return __TIME[1] + (__TIME[2] / 1000000) end)())'

        # Perform string replacement
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
            line = '%s(%s)' % ('self.' + node.func.attr, args)

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
            cmd = node.func.attr
            if cmd == 'delete':
                cmd = 'del'
            call = 'redis.call(\'%s\', %s)' % (cmd, args)

            # Wrap the Redis call in a function which stores the
            # result if needed later for pipelining and returns it
            expr = self.process_node(node.func.value).code
            line = '__PIPE_ADD(\'%s\', %s)' % (expr, call)
        else:
            # XXX Something we can't handle
            raise UntranslatableCodeException(node)

        code.append(LuaLine(line, node, indent))

    def process_Compare(self, node, code, indent, loops):
        """Generate code for a comparison operation"""

        # XXX We only handle a single comparison
        if len(node.ops) != 1 or len(node.comparators) != 1:
            raise UntranslatableCodeException(node)

        lhs = self.process_node(node.left).code

        if isinstance(node.ops[0], ast.Eq):
            op = ' == '
        elif isinstance(node.ops[0], ast.NotEq):
            op = ' ~= '
        elif isinstance(node.ops[0], ast.Gt):
            op = ' > '
        elif isinstance(node.ops[0], ast.GtE):
            op = ' >= '
        elif isinstance(node.ops[0], ast.Lt):
            op = ' < '
        elif isinstance(node.ops[0], ast.LtE):
            op = ' <= '
        else:
            # XXX We don't handle this type of comparison
            raise UntranslatableCodeException(node)

        rhs = self.process_node(node.comparators[0]).code
        line = '%s %s %s' % (lhs, op, rhs)
        code.append(LuaLine(line, node, indent))

    def process_Break(self, node, code, indent, loops):
        """Generate code for a break statement"""

        if LUA_DEBUG:
            code.append(LuaLine.debug('LOOP BREAK'))

        # Set the break flag for the current loop
        code.append(LuaLine('__BREAK%d = true' % loops, [], indent))
        code.append(LuaLine('do break end', [], indent))

    def process_Continue(self, node, code, indent, loops):
        """Generate code for a continue statement"""

        if LUA_DEBUG:
            code.append(LuaLine.debug('LOOP CONTINUE'))

        # We use the hack below of nested loops to implement continue,
        # so we just break out of that inner loop here
        # http://stackoverflow.com/a/25781200/123695
        # We have to embed this in a dummy conditional since for syntactic
        # reasons, break must always be the last statement in a block
        code.append(LuaLine('if true then break end', [], indent))

    def process_Dict(self, node, code, indent, loops):
        """Generate code for a dictionary literal"""

        pairs = ["['__DICT'] = true"]
        for key, value in itertools.izip(node.keys, node.values):
            key = self.process_node(key).code
            value = self.process_node(value).code
            pairs.append('[%s] = %s' % (key, value))

        line = '({%s})' % ', '.join(pairs)
        code.append(LuaLine(line, [], indent))

    def process_Expr(self, node, code, indent, loops):
        """Generate code for an expression"""

        code.append(self.process_node(node.value, indent))

    def process_For(self, node, code, indent, loops):
        """Generate code for a for loop"""

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

        if LUA_DEBUG:
            code.append(LuaLine.debug('STARTING LOOP OVER %s' % for_list))

        # Increment the loop counter and initialize the break flag
        loops += 1
        code.append(LuaLine('local __BREAK%d = false' % loops, node, indent))

        # Add the start of the for statement
        code.append(LuaLine(line, node, indent))

        # Trigger a break if the flag was set
        code.append(LuaLine('if __BREAK%d then break end' % loops,
            node, indent + 1))

        # Add a nested loop with only one iteration
        # which will allow us to break out when needed
        code.append(LuaLine('repeat', node, indent + 1))

        # Add all statements in the body
        for n in node.body:
            code.append(self.process_node(n, indent + 2, loops))

        # End the nested loop from above
        code.append(LuaLine('until true', node, indent + 1))

        # End the outer loop
        code.append(LuaLine('end', [], indent))

    def process_If(self, node, code, indent, loops):
        """Generate code for an if statement"""
        # Generate code for the test expression
        test = self.process_node(node.test).code

        if LUA_DEBUG:
            code.append(LuaLine.debug('CHECKING CONDITION %s' % test))

        # Add a line for the initial test
        line = 'if %s then' % test
        code.append(LuaLine(line, node, indent))

        if LUA_DEBUG:
            code.append(LuaLine.debug('CONDITION TRUE'))

        # Generate the body of the if block
        for n in node.body:
            code.append(self.process_node(n, indent + 1, loops))

        # Generate the body of the else branch
        if len(node.orelse) > 0:
            code.append(LuaLine('else', [], indent))
            code.append(LuaLine.debug('CONDITION FALSE, ELSE'))

        for n in node.orelse:
            code.append(self.process_node(n, indent + 1, loops))

        # Close the if block
        code.append(LuaLine('end', [], indent))

    def process_Index(self, node, code, indent, loops):
        """Generate code for an index value"""

        return self.process_Expr(node, code, indent, loops)

    def process_List(self, node, code, indent, loops):
        """Generate code for a list constant"""

        line = '{' + \
               ', '.join(self.process_node(n).code for n in node.elts) + '}'
        code.append(LuaLine(line, node, indent))

    def process_Name(self, node, code, indent, loops):
        """Generate code for a simple variable name"""

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

    def process_Num(self, node, code, indent, loops):
        """Generate code for a numberical constant"""

        line = self.convert_value(node.n)
        code.append(LuaLine(line, node, indent))

    def process_Pass(self, node, code, indent, loops):
        """Generate code for `pass`"""

        line = 'do end'
        code.append(LuaLine(line, node, indent))

    def process_Print(self, node, code, indent, loops):
        """Generate code for a print statement"""

        # XXX This changes behaviour to log to Redis instead
        #     of print on the application side

        # We only handle prints to stdout
        if node.dest is not None:
            raise UntranslatableCodeException(node)

        # Add a log statement for each print
        for value in node.values:
            value = self.process_node(value)
            line = 'redis.log(redis.LOG_DEBUG, %s)' % value
            code.append(LuaLine(line, node, indent))

            if LUA_DEBUG:
                code.append(LuaLine.debug('PRINT: %s', value))

    def process_Return(self, node, code, indent, loops):
        """Generate code for a return statement"""

        retval = self.process_node(node.value).code

        # If this is the final return value, pack it up with cmsgpack
        if self.helper:
            line = 'return %s' % retval
        else:
            line = 'return __RETVAL(%s, true)' % retval

        if LUA_DEBUG:
            code.append(LuaLine.debug('RETURNING %s', retval))

        code.append(LuaLine(line, node, indent))

    def process_Str(self, node, code, indent, loops):
        """Generate code for a string constant"""

        line = self.convert_value(node.s)
        code.append(LuaLine(line, node, indent))

    def process_Subscript(self, node, code, indent, loops):
        """Generate code for a subscript []"""

        subs = self.process_node(node.slice).code
        expr = self.process_node(node.value).code

        # Here we check the __DICT property of the object to see if
        # it is not a dictionary in which case we add 1 to the index
        line = '%s[(%s.__DICT) and (%s) or (%s + 1)]' % \
               (expr, expr, subs, subs)

        code.append(LuaLine(line, node, indent))

    def process_Tuple(self, node, code, indent, loops):
        """Generate code for a tuple constant"""

        return self.process_List(node, code, indent, loops)

    def process_UnaryOp(self, node, code, indent, loops):
        """Generate code for a unary operator"""

        operand = self.process_node(node.operand).code
        line = None

        if isinstance(node.op, ast.USub):
            op = '-'
        elif isinstance(node.op, ast.UAdd):
            # XXX We're assuming that unary addition does nothing
            op = ''
        elif isinstance(node.op, ast.Not):
            line = '(not __TRUE(%s))' % operand
        else:
            # XXX Some unhandled operator
            raise UntranslatableCodeException(node)

        if not line:
            line = '(%s%s)' % (op, self.process_node(node.operand).code)
        code.append(LuaLine(line, node, indent))

    def arg_conversion(self, arg):
        """Returns the function used to convert this argument to Lua"""

        if isinstance(arg, (int, long, float)):
            # Convert numbers from string form
            return 'tonumber'
        elif isinstance(arg, PACKED_TYPES):
            return 'cmsgpack.unpack'
        else:
            return ''

    def unpack_args(self, args, start_arg=0, helpers=[],
                    method_self=None):
        """Generate code to unpack arguments with the correct name and type"""

        # Unpack arguments to their original names performing
        # any necessary type conversions
        # XXX We assume arguments will always have the same type
        arg_unpacking = 'local self = {}\n'
        new_args = 0

        # Generate code for all helper functions
        helper_functions = ''
        for method_name in helpers:
            # We can skip Redis calls or calls to what we assume
            # are builtin functions
            if method_name[0] in map(lambda x: x.id, self.redis_objs) or \
               method_name[1] in FUNC_BUILTINS or \
               method_name == ('time', 'time'):
                continue

            # Anything else which is not a call on self is an error
            if method_name[0] != 'self':
                raise Exception()

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
            helper_functions += 'self.%s = function(%s)\n%s\nend\n' % \
                                (method_name[1],
                                 ', '.join(wrapped.arg_names), wrapped.body)

        for i, name in enumerate(self.in_exprs[start_arg:]):
            # Perform the lookup for class variables
            # We should be able to extend this to support multiple lookups
            # i.e., self.foo.bar
            if isinstance(name, tuple):
                if name[0] == 'self':
                    definition = 'self.%s' % name[1]
                    arg = getattr(method_self, name[1])
                else:
                    # XXX This shouldn't happen yet since we don't support
                    #     accessing things on objects other than self
                    raise Exception()
            else:
                definition = 'local %s' % self.in_exprs[i + start_arg]
                arg = args[i + start_arg]

            arg_unpacking += '%s = %s(ARGV[%d])\n' % \
                             (definition, self.arg_conversion(arg),
                              i + start_arg + 1)

            # Track if this is a dictionary so we know if we
            # need to add one to indexes into the Lua table
            if isinstance(arg, dict):
                expr = self.in_exprs[i + start_arg]
                if isinstance(expr, tuple):
                    expr = '.'.join(expr)

                arg_unpacking += '%s.__DICT = true\n' % expr

        # Expand any necessary helper arguments
        if new_args > 0:
            # Args is passed through as an empty array since all of them
            # must be pulled from method_self anyway
            helper_unpacking = self.unpack_args([], start_arg + new_args, [],
                                                method_self)
        else:
            helper_unpacking = ''

        return helper_unpacking + arg_unpacking + helper_functions

    def lua_code(self, client, args, method_self=None):
        """Produce the lua code for this script fragment"""

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
        # Register this script if needed
        if self.script_id is None:
            orig_args = copy.copy(args)
            self.register_script(*args)
        else:
            orig_args = args

        return self.taint.func(*orig_args)

    def register_script(self, *args):
        """Register the script with the client and patch the function code"""

        # Check if this is a method and pull the correct arguments
        if self.method:
            method_self = args[0]
            args = list(args[1:])
        else:
            method_self = None
            args = list(args)

        # Remove the client arguments from what is serialized and
        # pick the first client to actually use
        # XXX We do not actually support multiple different clients
        clients = []
        for arg in args[:]:
            if isinstance(arg, redis.StrictRedis):
                clients.append(arg)
                args.remove(arg)
        client = clients[0]

        lua_code = self.lua_code(client, args, method_self)
        self.script_id = ScriptRegistry.register_script(client, lua_code)

        # Get all the arguments to go to the function
        arg_exprs = copy.copy(self.arg_names)
        for attr in self.in_exprs:
            if isinstance(attr, tuple):
                arg_exprs.append('.'.join(attr))

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


def redis_server(method=None, redis_objs=None, minlineno=None, maxlineno=None):
    """Create a decorator which converts a function to run on the server"""

    def decorator(method):
        taint = sully.TaintAnalysis(method)
        fragment = RedisFuncFragment(taint, redis_objs=redis_objs,
                                     minlineno=minlineno, maxlineno=maxlineno)
        return functools.update_wrapper(fragment, method)

    return decorator(method) if method else decorator
