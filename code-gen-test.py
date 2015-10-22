import byteplay
import redis

# Convert arguments to their string values (right now just quotes constants)
def argstr(arg):
    if type(arg) == str:
        return "'%s'" % arg
    elif arg is None:
        return 'nil'
    else:
        return str(arg)

# Simple sentinel class used to signal loops
class Iter(object):
    def __init__(self, val):
        self.val = val

    def __repr__(self):
        return 'Iter(%s)' % str(self.val)

# Simple sentinel class used to signal conditions
class If(object):
    def __init__(self, val, label):
        self.val = val
        self.label = label

    def __repr__(self):
        return 'If(%s, %s)' % (str(self.val), str(self.label))

def redis_server(func):
    code = byteplay.Code.from_code(func.func_code)
    client_arg = code.args[0]
    arg_names = code.args[1:]

    stack = []
    for c in code.code:
        # Add the variable to the stack
        if c[0] == byteplay.LOAD_FAST:
            if c[1] in arg_names:
                stack.append('ARGV[%d]' % (arg_names.index(c[1]) + 1))
            else:
                stack.append(c[1])

        # Add the constant to the stack
        elif c[0] == byteplay.LOAD_CONST:
            stack.append(argstr(c[1]))

        # Add a tuple representing the attribute accessed on the item
        elif c[0] == byteplay.LOAD_ATTR:
            stack.append((stack.pop(), c[1]))

        # Generate code for array subscripting (add 1 since Lua starts there)
        elif c[0] == byteplay.BINARY_SUBSCR:
            index = stack.pop()
            stack.append('%s[%s + 1]' % (stack.pop(), index))

        # Generate code for add taking a dumb guess if this is string
        # concatenation or integer addition
        elif c[0] == byteplay.BINARY_ADD:
            op2 = stack.pop()
            if op2[0] == '"' or stack[-1][0] == "'":
                op = '..'
            else:
                op = '+'

            stack.append('%s %s %s' % (stack.pop(), op, op2))
        elif c[0] == byteplay.CALL_FUNCTION:
            # Get the number of arguments and remove them
            # and the function name from the stack
            nargs = c[1]
            args = stack[-nargs:]
            stack = stack[:-nargs]
            fn = stack.pop()

            # We assume all functions are called on objects and there
            # is no nested attribute access so fn is a tuple of
            # (object, function)
            if fn[0] == client_arg:
                # We're calling a redis function
                stack.append("redis.call('%s', %s)" % (fn[1], ', '.join(args)))
            elif fn[1] == 'append':
                # Code gen for list append
                stack.append("table.insert(%s, %s)" % (fn[0], ', '.join(args)))
            else:
                # XXX Not supported
                raise Exception()

        # Either store a value to a variable or start iterating
        elif c[0] == byteplay.STORE_FAST:
            val = stack.pop()

            # Put labels back on the stack
            if type(val) == byteplay.Label:
                label = val
                val = stack.pop()
                stack.append(label)

            if type(val) == Iter:
                # We popped an iterator, so we must be preparing to iterate
                # This assumes the following bytecode structure for loops
                #   SETUP_LOOP
                #   LOAD_FAST << this is the thing being iterated over
                #   GET_ITER
                #   FOR_ITER
                #   STORE_FAST << this is the variable for iteration
                stack.append('for _, %s in ipairs(%s) do' % (c[1], val.val))
            else:
                stack.append('local %s = %s' % (c[1], val))

        # Generate a new list constant
        elif c[0] == byteplay.BUILD_LIST:
            nargs = c[1]
            if nargs > 0:
                args = stack[-nargs:]
                stack = stack[:-nargs]
            else:
                args = []
            stack.append('{%s}' % ', '.join(args))

        # Create a new Iter object to signal iteration
        elif c[0] == byteplay.GET_ITER:
            stack.append(Iter(stack.pop()))

        # Just drop in an end statement to finish our block
        elif c[0] == byteplay.POP_BLOCK:
            stack.append('end')

        # Generate code for the return
        elif c[0] == byteplay.RETURN_VALUE:
            stack.append('return %s' % stack.pop())

        # Push an If onto the stack that we deal with later during output
        elif c[0] == byteplay.POP_JUMP_IF_FALSE:
            stack.append(If(stack.pop(), c[1]))

        # Push labels so we can do proper code gen for conditions
        elif type(c[0]) == byteplay.Label:
            stack.append(c[0])

        # Generate code for boolean comparisons
        elif c[0] == byteplay.COMPARE_OP:
            arg2, arg1 = stack.pop(), stack.pop()
            stack.append('%s %s %s' % (arg1, c[1], arg2))

    if_labels = []
    lua_code = ''
    indent = 0
    for line in stack:
        # We found a condition so store the label so we know when to end
        if type(line) == If:
            if_labels.append(line.label)
            line = 'if %s then' % line.val

        # Remove the label and end the block if this was a condition
        if type(line) == byteplay.Label:
            if line in if_labels:
                if_labels.remove(line)
                line = 'end'
            else:
                continue

        if line == 'end':
            indent -= 1
        lua_code += '    ' * indent + line + '\n'
        if line.endswith(' do') or line.startswith('if '):
            indent += 1

    func.script = None
    def inner(client, *args):
        if func.script is None:
            func.script = client.register_script(lua_code)

        return func.script(args=args)

    return inner

@redis_server
def get_by_category(client, category):
    ids = client.lrange('category:' + category, 0, -1)
    items = []
    for id in ids:
        items.append(client.hget(id, 'name'))
    return items

@redis_server
def increx(client, key):
    if client.exists(key) == 1:
        return client.incr(key)

@redis_server
def hincrex(client, key, field):
    if client.hexists(key, field) == 1:
        return client.hincrby(key, field, 1)

@redis_server
def add_link(client, url):
    link_id = client.incr('counter')
    client.hset('links', link_id, url)
    return link_id

client = redis.StrictRedis()

print(increx(client, 'foo'))
client.set('foo', 1)
print(increx(client, 'foo'))

print(hincrex(client, 'bar', 'baz'))
client.hset('bar', 'baz', 1)
print(hincrex(client, 'bar', 'baz'))

client.hmset('item:1', { 'name': 'Foo', 'category': 'Bar' })
client.lpush('category:Bar', 'item:1')
print(get_by_category(client, 'Bar'))

print(add_link(client, 'foo'))
print(add_link(client, 'foo'))

client.script_flush()
client.flushall()
