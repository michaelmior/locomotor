import inspect
import compiler
import types

TAB = '  '
SELF_ARG = 'self__'

class RedisFunc(object):
    def __init__(self, func, helper=False):
        self.func = func

        # Get the source code and strip whitespace and decorators
        source = inspect.getsourcelines(func)[0]
        spaces = len(source[0]) - len(source[0].lstrip())
        source = [line[spaces:] for line in source]
        self.source = ''.join(line for line in source if line[0] != '@')

        # Generate the AST and do some munging to get the node that represents
        # the body of the function which is all that we care about
        ast = compiler.parse(self.source)
        self.arg_names = ast.node.nodes[0].argnames
        self.ast = ast.node.nodes[0].getChildNodes()[0]

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

        # Strip the instance and client object parameters
        self.arg_names = list(self.arg_names[self.method + (not helper):])

        # Generate the code for the body of the method
        self.body = self.process_node(self.ast, 1 if helper else 0)

        # Initialize the script to None, we'll register it Later
        self.script = None

    # Generate code for a single node at a particular indentation level
    def process_node(self, node, indent=0):
        code = ''

        if node.__class__ == compiler.ast.Stmt:
            for n in node.getChildNodes():
                code += self.process_node(n, indent)
        elif node.__class__ == compiler.ast.Assign:
            value = self.process_node(node.expr, indent).strip()

            for var in node.nodes:
                code += TAB * indent + 'local %s = %s\n' % (var.name, value)
        elif node.__class__ == compiler.ast.List:
            code = TAB * indent + '{' + \
                ', '.join(self.process_node(n) for n in node.nodes) + '}'
        elif node.__class__ == compiler.ast.Return:
            code = TAB * indent + 'return ' + self.process_node(node.value)
        elif node.__class__ == compiler.ast.Name:
            # None is represented as a name, but we want nil
            if node.name == 'None':
                node.name = 'nil'

            code = TAB * indent + node.name
        elif node.__class__ == compiler.ast.For:
            # Get the list we are looping over
            for_list = self.process_node(node.list)

            # Try to find a comma in the list
            try:
                comma_index = for_list.index(',')
            except ValueError:
                comma_index = False

            # This is a dumb heuristic and we just propagate this information
            # if we were more careful, but we check for a digit followed by
            # a comma to see if this is a loop over a range or a list
            if comma_index is not False and for_list[0:comma_index].isdigit():
                code = TAB * indent + 'for %s=%s do\n' % \
                        (node.assign.name, for_list)
            else:
                code = TAB * indent + 'for _, %s in ipairs(%s) do\n' % \
                    (node.assign.name, for_list)

            code += self.process_node(node.body, indent + 1)
            code += TAB * indent + 'end\n'
        elif node.__class__ == compiler.ast.Discard:
            code = self.process_node(node.expr, indent)
        elif node.__class__ == compiler.ast.UnarySub:
            code = TAB * indent + '-' + self.process_node(node.expr)
        elif node.__class__ == compiler.ast.Add:
            op1 = self.process_node(node.left)
            op2 = self.process_node(node.right)

            # Guess if either operand is a string
            if op1[0] == "'" or op2[0] == "'":
                op = ' .. '
            else:
                op = ' + '

            code = TAB * indent + op1 + op + op2
        elif node.__class__ == compiler.ast.Const:
            if node.value is None:
                code = 'nil'
            elif type(node.value) is str:
                code = "'" + node.value + "'"
            else:
                code = str(node.value)

            code = TAB * indent + code
        elif node.__class__ == compiler.ast.CallFunc:
            # We don't support positional or keyword arguments
            if node.star_args or node.dstar_args:
                raise Exception()

            args = ', '.join(self.process_node(n) for n in node.args)

            # Handle some built-in functions
            if node.node.__class__ == compiler.ast.Name:
                if node.node.name == 'int':
                    code = 'tonumber(%s)' % args
                elif node.node.name == 'str':
                    code = 'tostring(%s)' % args
                elif node.node.name in ('range', 'xrange'):
                    # Extend to always use three arguments
                    if len(node.args) == 1:
                        args = '0, %s - 1, 1' % args
                    elif len(node.args) == 2:
                        args += ' - 1, 1'

                    code = args
                else:
                    # XXX We don't know how to handle this function
                    raise Exception()

            # XXX We assume now that the function being called
            #     is a GetAttr node

            # Check if we have a method call
            elif node.node.expr.name == 'self':
                # Add this function like a new argument
                new_arg = SELF_ARG + node.node.attrname
                if new_arg not in self.arg_names:
                    self.arg_names.append(new_arg)

                code = '%s(%s)' % (new_arg, args)

            # If we're calling append, add to the end of a list
            elif node.node.attrname == 'append':
                code = 'table.insert(%s, %s)\n' \
                        % (self.process_node(node.node.expr), args)

            # XXX Otherwise, assume this is a redis function call
            else:
                code = 'redis.call(\'%s\', %s)' % (node.node.attrname, args)

            code = TAB * indent + code
        elif node.__class__ == compiler.ast.If:
            # It seems that the tests array always has one element in which
            # is a two element list that contains the test and the body of
            # the statement
            if len(node.tests) != 1 or len(node.tests[0]) != 2:
                raise Exception()

            # TODO: Handle else
            if node.else_ is not None:
                raise Exception()

            test = self.process_node(node.tests[0][0])
            code = TAB * indent + 'if %s then\n%s\n' % \
                    (test, self.process_node(node.tests[0][1], indent + 1))
            code += TAB * indent + 'end\n'
        elif node.__class__ == compiler.ast.Compare:
            # The ops attribute should contain an array with a single element which
            # is a two element list containing the comparison operator and the
            # value to be compared with
            if len(node.ops) != 1 or len(node.ops[0]) != 2:
                raise Exception()

            lhs = self.process_node(node.expr)
            op = node.ops[0][0]
            rhs = self.process_node(node.ops[0][1])
            code = '%s %s %s' % (lhs, op, rhs)
        elif node.__class__ == compiler.ast.Getattr:
            obj = self.process_node(node.expr)

            if obj != 'self':
                # XXX We're probably doing some external stuff we can't handle
                raise Exception()

            # Add a new argument to handle this
            new_arg = SELF_ARG + node.attrname
            if new_arg not in self.arg_names:
                self.arg_names.append(new_arg)
            code = new_arg
        else:
            # XXX This type of node is not handled
            raise Exception()

        return code

    # Register the script with the backend
    def register_script(self, client, args, method_self=None):
        # Unpack arguments to their original names performing
        # any necessary type conversions
        # XXX We assume arguments will always have the same type
        arg_unpacking = ''
        for i, name in enumerate(self.arg_names):
            # Perform the lookup for class variables
            # We should be able to extend this to support multiple lookups
            # i.e., self.foo.bar
            if name.startswith(SELF_ARG):
                arg = getattr(method_self, name[len(SELF_ARG):])
            else:
                arg = args[i]

            # Generate code for methods called within this method
            if isinstance(arg, types.MethodType):
                # XXX We currently assume that methods called by the method
                #     we're translating do not access any attributes of
                #     the instance
                wrapped = RedisFunc(arg, helper=True)

                # Dump the helper function code into a local variable
                arg_unpacking += 'local %s = function(%s)\n%s\nend\n' % \
                        (self.arg_names[i], ', '.join(wrapped.arg_names), wrapped.body)
                continue

            # Convert numbers from string form
            elif isinstance(arg, (int, long, float)):
                conversion = 'tonumber'
            else:
                conversion = ''

            arg_unpacking += 'local %s = %s(ARGV[%d])\n' % \
                    (self.arg_names[i], conversion ,i+1)

        self.script = client.register_script(arg_unpacking + self.body)

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

        return self.script(args=args)

redis_server = RedisFunc
