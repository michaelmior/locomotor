local cmsgpack = require 'cmsgpack'
local redis = require 'redis'
 
local host = "127.0.0.1"
local port = 6379
 
client = redis.connect(host, port)

-- redis.replicate_commands()
local __RETVAL = function(value, retval)
  local __RESULT = {}
  __RESULT["__value"] = value
  __RESULT["__return"] = retval

  return cmsgpack.pack(__RESULT)
end

local tpcc = function(KEYS, ARGV)
  local __TRUE = function(expr)
    local __VAL = expr
    if not __VAL or __VAL == 0 then
      return false
    end
  
    local __TYPE = type(__VAL)
    if (__TYPE == "table" or __TYPE == "string") and #__VAL == 0 then
        return false
    else
        return true
    end
  end
  
  local __OR = function(...)
      local __VAL = nil
      for i, v in ipairs(arg) do
          if __TRUE(v) then
              return v
          else
              __VAL = v
          end
      end
  
      return __VAL
  end
  
  local __AND = function(...)
      local __VAL = nil
      for i, v in ipairs(arg) do
          if not __TRUE(v) then
              return v
          else
              __VAL = v
          end
      end
  
      return __VAL
  end
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
  
  local self = {}
  self.debug = cmsgpack.unpack(ARGV[2])
  self.debug.__DICT = true
  self.KEY_SEPARATOR = (ARGV[3])
  local self = {}
  local params = cmsgpack.unpack(ARGV[1])
  params.__DICT = true
  self.debug = cmsgpack.unpack(ARGV[2])
  self.debug.__DICT = true
  self.KEY_SEPARATOR = (ARGV[3])
  self.safeKey = function(keys)
  local new_keys;
    new_keys = {};
    for _, k in ipairs(keys) do
      repeat
        table.insert(new_keys, tostring(k))
      until true
    end
    return ((function() local __TEMP, _; __TEMP, _ = string.gsub(((function() local __TEMP, _; __TEMP, _ = string.gsub(table.concat(new_keys, self.KEY_SEPARATOR)
  , '\n', ''); return __TEMP end)()), ' ', ''); return __TEMP end)())
  
  end
  local c_id;
  local no_key;
  local customer_key;
  local no_o_id;
  local new_balance;
  local result;
  local si_key;
  local id_set;
  local order_key;
  local index;
  local no_si_key;
  local ol_delivery_d;
  local index_key;
  local tt;
  local counter;
  local ol_ids;
  local t0;
  local old_balance;
  local cursor;
  local ol_counts;
  local w_id;
  local pipe_results;
  local ol_total;
  local o_carrier_id;
  if self.debug[(self.debug.__DICT) and ('delivery') or ('delivery' + 1)]  ~=  'None' then
    redis.log(redis.LOG_DEBUG, 'TXN DELIVERY STARTING ------------------'
  )
    tt = ((function() local __TIME = client:time(); return __TIME[1] + (__TIME[2] / 1000000) end)());
  end
  if self.debug[(self.debug.__DICT) and ('delivery') or ('delivery' + 1)]  ==  'Verbose' then
    t0 = tt;
  end
  w_id = params[(params.__DICT) and ('w_id') or ('w_id' + 1)];
  o_carrier_id = params[(params.__DICT) and ('o_carrier_id') or ('o_carrier_id' + 1)];
  ol_delivery_d = params[(params.__DICT) and ('ol_delivery_d') or ('ol_delivery_d' + 1)];
  result = {};
  order_key = {};
  ol_total = {};
  customer_key = {};
  ol_counts = {};
  no_o_id = {};
  for d_id=1, 10 + 1 - 1, 1 do
    repeat
      table.insert(order_key, nil)
      table.insert(ol_total, 0)
      table.insert(customer_key, nil)
      table.insert(ol_counts, 0)
    until true
  end
  for d_id=1, 10 + 1 - 1, 1 do
    repeat
      cursor = d_id - 1;
      index_key = self.safeKey({d_id, w_id});
      __PIPE_ADD('rdr', client:srandmember('NEW_ORDER.INDEXES.GETNEWORDER.' .. index_key))
    until true
  end
  id_set = __PIPE_GET('rdr');
  for d_id=1, 10 + 1 - 1, 1 do
    repeat
      cursor = d_id - 1;
      if (not __TRUE(id_set[(id_set.__DICT) and (cursor) or (cursor + 1)])) then
        __PIPE_ADD('rdr', client:get('NULL_VALUE'))
      else
        __PIPE_ADD('rdr', client:hget('NEW_ORDER.' .. tostring(id_set[(id_set.__DICT) and (cursor) or (cursor + 1)]), 'NO_O_ID'))
      end
    until true
  end
  no_o_id = __PIPE_GET('rdr');
  if self.debug[(self.debug.__DICT) and ('delivery') or ('delivery' + 1)]  ==  'Verbose' then
    redis.log(redis.LOG_DEBUG, 'New Order Query: '
  )
    redis.log(redis.LOG_DEBUG, ((function() local __TIME = client:time(); return __TIME[1] + (__TIME[2] / 1000000) end)()) - t0
  )
    t0 = ((function() local __TIME = client:time(); return __TIME[1] + (__TIME[2] / 1000000) end)());
  end
  for d_id=1, 10 + 1 - 1, 1 do
    repeat
      cursor = d_id - 1;
      if (not __TRUE(no_o_id[(no_o_id.__DICT) and (cursor) or (cursor + 1)])) then
        order_key[(order_key.__DICT) and (cursor) or (cursor + 1)] = 'NO_KEY';
      else
        order_key[(order_key.__DICT) and (cursor) or (cursor + 1)] = self.safeKey({w_id, d_id, no_o_id[(no_o_id.__DICT) and (0) or (0 + 1)]});
      end
      __PIPE_ADD('rdr', client:hget('ORDERS.' .. order_key[(order_key.__DICT) and (cursor) or (cursor + 1)], 'O_C_ID'))
    until true
  end
  c_id = __PIPE_GET('rdr');
  for d_id=1, 10 + 1 - 1, 1 do
    repeat
      cursor = d_id - 1;
      if __OR(((not __TRUE(no_o_id[(no_o_id.__DICT) and (cursor) or (cursor + 1)]))), ((not __TRUE(c_id[(c_id.__DICT) and (cursor) or (cursor + 1)])))) then
        si_key = 'NO_KEY';
      else
        si_key = self.safeKey({no_o_id[(no_o_id.__DICT) and (cursor) or (cursor + 1)], d_id, w_id});
      end
      __PIPE_ADD('rdr', client:smembers('ORDER_LINE.INDEXES.SUMOLAMOUNT.' .. si_key))
    until true
  end
  ol_ids = __PIPE_GET('rdr');
  if self.debug[(self.debug.__DICT) and ('delivery') or ('delivery' + 1)]  ==  'Verbose' then
    redis.log(redis.LOG_DEBUG, 'Get Customer ID Query:'
  )
    redis.log(redis.LOG_DEBUG, ((function() local __TIME = client:time(); return __TIME[1] + (__TIME[2] / 1000000) end)()) - t0
  )
    t0 = ((function() local __TIME = client:time(); return __TIME[1] + (__TIME[2] / 1000000) end)());
  end
  for d_id=1, 10 + 1 - 1, 1 do
    repeat
      cursor = d_id - 1;
      if __OR(((not __TRUE(no_o_id[(no_o_id.__DICT) and (cursor) or (cursor + 1)]))), ((not __TRUE(c_id[(c_id.__DICT) and (cursor) or (cursor + 1)])))) then
        __PIPE_ADD('rdr', client:get('NULL_VALUE'))
      else
        for _, i in ipairs(ol_ids[(ol_ids.__DICT) and (cursor) or (cursor + 1)]) do
          repeat
            __PIPE_ADD('rdr', client:hget('ORDER_LINE.' .. tostring(i), 'OL_AMOUNT'))
            ol_counts[(ol_counts.__DICT) and (cursor) or (cursor + 1)] = ol_counts[(ol_counts.__DICT) and (cursor) or (cursor + 1)] + 1
          until true
        end
      end
    until true
  end
  pipe_results = __PIPE_GET('rdr');
  index = 0;
  counter = 0;
  for _, ol_amount in ipairs(pipe_results) do
    repeat
      counter = counter + 1
      if counter  >  ol_counts[(ol_counts.__DICT) and (index) or (index + 1)] then
        index = index + 1
        counter = 0;
      else
        if ol_amount then
          ol_total[(ol_total.__DICT) and (index) or (index + 1)] = ol_total[(ol_total.__DICT) and (index) or (index + 1)] + tonumber(ol_amount)
        end
      end
    until true
  end
  if self.debug[(self.debug.__DICT) and ('delivery') or ('delivery' + 1)]  ==  'Verbose' then
    redis.log(redis.LOG_DEBUG, 'Sum Order Line Query:'
  )
    redis.log(redis.LOG_DEBUG, ((function() local __TIME = client:time(); return __TIME[1] + (__TIME[2] / 1000000) end)()) - t0
  )
    t0 = ((function() local __TIME = client:time(); return __TIME[1] + (__TIME[2] / 1000000) end)());
  end
  for d_id=1, 10 + 1 - 1, 1 do
    repeat
      cursor = d_id - 1;
      if __OR(((not __TRUE(no_o_id[(no_o_id.__DICT) and (cursor) or (cursor + 1)]))), ((not __TRUE(c_id[(c_id.__DICT) and (cursor) or (cursor + 1)])))) then
        if true then break end
      end
      no_key = self.safeKey({d_id, w_id, no_o_id[(no_o_id.__DICT) and (cursor) or (cursor + 1)]});
      no_si_key = self.safeKey({d_id, w_id});
      __PIPE_ADD('wtr', client:del('NEW_ORDER.' .. no_key))
      __PIPE_ADD('wtr', client:srem('NEW_ORDER.IDS', no_key))
      __PIPE_ADD('wtr', client:srem('NEW_ORDER.INDEXES.GETNEWORDER.' .. no_si_key, no_key))
      if self.debug[(self.debug.__DICT) and ('delivery') or ('delivery' + 1)]  ==  'Verbose' then
        redis.log(redis.LOG_DEBUG, 'Delete New Order Query:'
  )
        redis.log(redis.LOG_DEBUG, ((function() local __TIME = client:time(); return __TIME[1] + (__TIME[2] / 1000000) end)()) - t0
  )
        t0 = ((function() local __TIME = client:time(); return __TIME[1] + (__TIME[2] / 1000000) end)());
      end
      __PIPE_ADD('wtr', client:hset('ORDERS.' .. order_key[(order_key.__DICT) and (cursor) or (cursor + 1)], 'W_CARRIER_ID', o_carrier_id))
      if self.debug[(self.debug.__DICT) and ('delivery') or ('delivery' + 1)]  ==  'Verbose' then
        redis.log(redis.LOG_DEBUG, 'Update Orders Query:'
  )
        redis.log(redis.LOG_DEBUG, ((function() local __TIME = client:time(); return __TIME[1] + (__TIME[2] / 1000000) end)()) - t0
  )
        t0 = ((function() local __TIME = client:time(); return __TIME[1] + (__TIME[2] / 1000000) end)());
      end
      for _, i in ipairs(ol_ids[(ol_ids.__DICT) and (cursor) or (cursor + 1)]) do
        repeat
          __PIPE_ADD('wtr', client:hset('ORDER_LINE.' .. tostring(i), 'OL_DELIVERY_D', ol_delivery_d))
          if self.debug[(self.debug.__DICT) and ('delivery') or ('delivery' + 1)]  ==  'Verbose' then
            redis.log(redis.LOG_DEBUG, 'Update Order Line Query:'
  )
            redis.log(redis.LOG_DEBUG, ((function() local __TIME = client:time(); return __TIME[1] + (__TIME[2] / 1000000) end)()) - t0
  )
          end
          t0 = ((function() local __TIME = client:time(); return __TIME[1] + (__TIME[2] / 1000000) end)());
        until true
      end
    until true
  end
  __PIPE_GET('wtr')
  for d_id=1, 10 + 1 - 1, 1 do
    repeat
      cursor = d_id - 1;
      if __OR(((not __TRUE(no_o_id[(no_o_id.__DICT) and (cursor) or (cursor + 1)]))), ((not __TRUE(c_id[(c_id.__DICT) and (cursor) or (cursor + 1)])))) then
        __PIPE_ADD('rdr', client:get('NULL_VALUE'))
        customer_key[(customer_key.__DICT) and (cursor) or (cursor + 1)] = 'NO_KEY';
      else
        customer_key[(customer_key.__DICT) and (cursor) or (cursor + 1)] = self.safeKey({w_id, d_id, c_id[(c_id.__DICT) and (cursor) or (cursor + 1)]});
        __PIPE_ADD('rdr', client:hget('CUSTOMER.' .. customer_key[(customer_key.__DICT) and (cursor) or (cursor + 1)], 'C_BALANCE'))
      end
    until true
  end
  old_balance = __PIPE_GET('rdr');
  for d_id=1, 10 + 1 - 1, 1 do
    repeat
      cursor = d_id - 1;
      if __OR(((not __TRUE(no_o_id[(no_o_id.__DICT) and (cursor) or (cursor + 1)]))), ((not __TRUE(c_id[(c_id.__DICT) and (cursor) or (cursor + 1)])))) then
        if true then break end
      else
        new_balance = tonumber(old_balance[(old_balance.__DICT) and (cursor) or (cursor + 1)]) + tonumber(ol_total[(ol_total.__DICT) and (cursor) or (cursor + 1)]);
        __PIPE_ADD('wtr', client:hset('CUSTOMER.' .. customer_key[(customer_key.__DICT) and (cursor) or (cursor + 1)], 'C_BALANCE', new_balance))
        table.insert(result, {d_id, no_o_id[(no_o_id.__DICT) and (cursor) or (cursor + 1)]})
      end
    until true
  end
  __PIPE_GET('wtr')
  if self.debug[(self.debug.__DICT) and ('delivery') or ('delivery' + 1)]  ==  'Verbose' then
    redis.log(redis.LOG_DEBUG, 'Update Customer Query:'
  )
    redis.log(redis.LOG_DEBUG, ((function() local __TIME = client:time(); return __TIME[1] + (__TIME[2] / 1000000) end)()) - t0
  )
  end
  if self.debug[(self.debug.__DICT) and ('delivery') or ('delivery' + 1)]  ~=  'None' then
    redis.log(redis.LOG_DEBUG, 'TXN DELIVERY:'
  )
    redis.log(redis.LOG_DEBUG, ((function() local __TIME = client:time(); return __TIME[1] + (__TIME[2] / 1000000) end)()) - tt
  )
  end
  return __RETVAL(result, true)
end

local generateDeliveryParams = function()
  w_id = math.random(1, 4)
  o_carrier_id = math.random(1, 10)
  ol_delivery_d = os.time()

  return {w_id=w_id, o_carrier_id= o_carrier_id, ol_delivery_d= ol_delivery_d}
end

for i=1,10000 do
    tpcc({}, {cmsgpack.pack(generateDeliveryParams()), cmsgpack.pack({delivery="None"}), ":"})
end
