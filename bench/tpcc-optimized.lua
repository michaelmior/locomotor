local redis = require 'redis'
 
local host = "127.0.0.1"
local port = 6379
 
client = redis.connect(host, port)

-- redis.replicate_commands()

local tpcc = function(params)
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
  self.safeKey = function(keys)
  local new_keys;
    new_keys = {};
    for _, k in ipairs(keys) do
      repeat
        table.insert(new_keys, tostring(k))
      until true
    end
    return ((function() local __TEMP, _; __TEMP, _ = string.gsub(((function() local __TEMP, _; __TEMP, _ = string.gsub(table.concat(new_keys, ":")
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
  w_id = params['w_id'];
  o_carrier_id = params['o_carrier_id'];
  ol_delivery_d = params['ol_delivery_d'];
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
      if (not id_set[cursor + 1]) then
        __PIPE_ADD('rdr', client:get('NULL_VALUE'))
      else
        __PIPE_ADD('rdr', client:hget('NEW_ORDER.' .. tostring(id_set[cursor + 1]), 'NO_O_ID'))
      end
    until true
  end
  no_o_id = __PIPE_GET('rdr');
  for d_id=1, 10 + 1 - 1, 1 do
    repeat
      cursor = d_id - 1;
      if (not no_o_id[cursor + 1]) then
        order_key[cursor + 1] = 'NO_KEY';
      else
        order_key[cursor + 1] = self.safeKey({w_id, d_id, no_o_id[1]});
      end
      __PIPE_ADD('rdr', client:hget('ORDERS.' .. order_key[cursor + 1], 'O_C_ID'))
    until true
  end
  c_id = __PIPE_GET('rdr');
  for d_id=1, 10 + 1 - 1, 1 do
    repeat
      cursor = d_id - 1;
      if ((not no_o_id[cursor + 1])) or ((not c_id[cursor + 1])) then
        si_key = 'NO_KEY';
      else
        si_key = self.safeKey({no_o_id[cursor + 1], d_id, w_id});
      end
      __PIPE_ADD('rdr', client:smembers('ORDER_LINE.INDEXES.SUMOLAMOUNT.' .. si_key))
    until true
  end
  ol_ids = __PIPE_GET('rdr');
  for d_id=1, 10 + 1 - 1, 1 do
    repeat
      cursor = d_id - 1;
      if ((not no_o_id[cursor + 1])) or ((not c_id[cursor + 1])) then
        __PIPE_ADD('rdr', client:get('NULL_VALUE'))
      else
        for _, i in ipairs(ol_ids[cursor + 1]) do
          repeat
            __PIPE_ADD('rdr', client:hget('ORDER_LINE.' .. tostring(i), 'OL_AMOUNT'))
            ol_counts[cursor + 1] = ol_counts[cursor + 1] + 1
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
      if counter  >  ol_counts[index + 1] then
        index = index + 1
        counter = 0;
      else
        if ol_amount then
          ol_total[index + 1] = ol_total[index + 1] + tonumber(ol_amount)
        end
      end
    until true
  end
  for d_id=1, 10 + 1 - 1, 1 do
    repeat
      cursor = d_id - 1;
      if ((not no_o_id[cursor + 1])) or ((not c_id[cursor + 1])) then
        if true then break end
      end
      no_key = self.safeKey({d_id, w_id, no_o_id[cursor + 1]});
      no_si_key = self.safeKey({d_id, w_id});
      __PIPE_ADD('wtr', client:del('NEW_ORDER.' .. no_key))
      __PIPE_ADD('wtr', client:srem('NEW_ORDER.IDS', no_key))
      __PIPE_ADD('wtr', client:srem('NEW_ORDER.INDEXES.GETNEWORDER.' .. no_si_key, no_key))
      __PIPE_ADD('wtr', client:hset('ORDERS.' .. order_key[cursor + 1], 'W_CARRIER_ID', o_carrier_id))
      for _, i in ipairs(ol_ids[cursor + 1]) do
        repeat
          __PIPE_ADD('wtr', client:hset('ORDER_LINE.' .. tostring(i), 'OL_DELIVERY_D', ol_delivery_d))
          t0 = ((function() local __TIME = client:time(); return __TIME[1] + (__TIME[2] / 1000000) end)());
        until true
      end
    until true
  end
  __PIPE_GET('wtr')
  for d_id=1, 10 + 1 - 1, 1 do
    repeat
      cursor = d_id - 1;
      if ((not no_o_id[cursor + 1])) or ((not c_id[cursor + 1])) then
        __PIPE_ADD('rdr', client:get('NULL_VALUE'))
        customer_key[cursor + 1] = 'NO_KEY';
      else
        customer_key[cursor + 1] = self.safeKey({w_id, d_id, c_id[cursor + 1]});
        __PIPE_ADD('rdr', client:hget('CUSTOMER.' .. customer_key[cursor + 1], 'C_BALANCE'))
      end
    until true
  end
  old_balance = __PIPE_GET('rdr');
  for d_id=1, 10 + 1 - 1, 1 do
    repeat
      cursor = d_id - 1;
      if ((not no_o_id[cursor + 1])) or ((not c_id[cursor + 1])) then
        if true then break end
      else
        new_balance = tonumber(old_balance[cursor + 1]) + tonumber(ol_total[cursor + 1]);
        __PIPE_ADD('wtr', client:hset('CUSTOMER.' .. customer_key[cursor + 1], 'C_BALANCE', new_balance))
        table.insert(result, {d_id, no_o_id[cursor + 1]})
      end
    until true
  end
  __PIPE_GET('wtr')
  return result
end

local generateDeliveryParams = function()
  w_id = math.random(1, 4)
  o_carrier_id = math.random(1, 10)
  ol_delivery_d = os.time()

  return {w_id=w_id, o_carrier_id= o_carrier_id, ol_delivery_d= ol_delivery_d}
end

require 'chronos'
local start = chronos.nanotime()

for i=1,10000 do
    tpcc(generateDeliveryParams())
end

local stop = chronos.nanotime()
print(("Completed in %s seconds"):format(stop - start))
