import byteplay
import inspect
import compiler

TAB = '  '

# Generate code for a single node at a particular indentation level
def process_node(node, arg_names, indent=0):
    code = ''

    if node.__class__ == compiler.ast.Stmt:
        for n in node.getChildNodes():
            code += process_node(n, arg_names, indent)
    elif node.__class__ == compiler.ast.Assign:
        value = process_node(node.expr, arg_names, indent).strip()

        for var in node.nodes:
            code += TAB * indent + 'local %s = %s\n' % (var.name, value)
    elif node.__class__ == compiler.ast.List:
        code = TAB * indent + '{' + \
            ', '.join(process_node(n, arg_names) for n in node.nodes) + '}'
    elif node.__class__ == compiler.ast.Return:
        code = TAB * indent + 'return ' + process_node(node.value, arg_names)
    elif node.__class__ == compiler.ast.Name:
        if node.name in arg_names:
            name = 'ARGV[%d]' % (arg_names.index(node.name) + 1)
        else:
            name = node.name
        code = TAB * indent + name
    elif node.__class__ == compiler.ast.For:
        for_list = process_node(node.list, arg_names)
        code = TAB * indent + 'for _, %s in ipairs(%s) do\n' % \
            (node.assign.name, for_list)
        code += process_node(node.body, arg_names, indent + 1)
        code += TAB * indent + 'end\n'
    elif node.__class__ == compiler.ast.Discard:
        code = process_node(node.expr, arg_names, indent)
    elif node.__class__ == compiler.ast.UnarySub:
        code = TAB * indent + '-' + process_node(node.expr, arg_names)
    elif node.__class__ == compiler.ast.Add:
        op1 = process_node(node.left, arg_names)
        op2 = process_node(node.right, arg_names)

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

        args = ', '.join(process_node(n, arg_names) for n in node.args)

        # Handle some built-in functions
        if node.node.__class__ == compiler.ast.Name:
            if node.node.name == 'int':
                code = 'tonumber(%s)' % args
            elif node.node.name == 'str':
                code = 'tostring(%s)' % args
            else:
                # XXX We don't know how to handle this function
                raise Exception()
        # XXX We assume now that the function being called is a GetAttr node

        # If we're calling append, add to the end of a list
        elif node.node.attrname == 'append':
            code = 'table.insert(%s, %s)\n' \
                    % (process_node(node.node.expr, arg_names), args)

        # XXX Otherwise, assume this is a redis function call
        else:
            code = 'redis.call(\'%s\', %s)' % (node.node.attrname, args)

        code = TAB * indent + code
    elif node.__class__ == compiler.ast.If:
        # It seems that the tests array always has one element in which is a
        # two element list that contains the test and the body of the statement
        if len(node.tests) != 1 or len(node.tests[0]) != 2:
            raise Exception()

        # TODO: Handle else
        if node.else_ is not None:
            raise Exception()

        test = process_node(node.tests[0][0], arg_names)
        code = TAB * indent + 'if %s then\n%s\n' % \
                (test, process_node(node.tests[0][1], arg_names, indent + 1))
        code += TAB * indent + 'end\n'
    elif node.__class__ == compiler.ast.Compare:
        # The ops attribute should contain an array with a single element which
        # is a two element list containing the comparison operator and the
        # value to be compared with
        if len(node.ops) != 1 or len(node.ops[0]) != 2:
            raise Exception()

        lhs = process_node(node.expr, arg_names)
        op = node.ops[0][0]
        rhs = process_node(node.ops[0][1], arg_names)
        code = '%s %s %s' % (lhs, op, rhs)
    else:
        # XXX This type of node is not handled
        raise Exception()

    return code

def redis_server(func):
    # Get the names of function arguments
    code = byteplay.Code.from_code(func.func_code)

    # Assume that this is a method if the first argument is self
    # This is obviously brittle, but easy and will probably work
    if code.args[0] == 'self':
        method = True
        client_arg = code.args[1]
        arg_names = code.args[2:]
    else:
        method = False
        client_arg = code.args[0]
        arg_names = code.args[1:]

    # Get the source code and strip whitespace and decorators
    source = inspect.getsourcelines(func)[0]
    spaces = len(source[0]) - len(source[0].lstrip())
    source = [line[spaces:] for line in source]
    source = ''.join(line for line in source if line[0] != '@')

    # Generate the AST and the corresponding Lua code
    ast = compiler.parse(source)
    lua_code = process_node(ast.node.nodes[0].getChildNodes()[0], arg_names, 0)

    func.script = None
    def inner(*args):
        if method:
            self = args[0]
            client = args[1]
            args = args[2:]
        else:
            client = args[0]
            args = args[1:]

        if func.script is None:
            func.script = client.register_script(lua_code)

        return func.script(args=args)

    return inner
